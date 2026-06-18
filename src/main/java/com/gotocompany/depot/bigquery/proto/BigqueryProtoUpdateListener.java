package com.gotocompany.depot.bigquery.proto;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.protobuf.Descriptors.Descriptor;
import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverter;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.exception.BQSchemaMappingException;
import com.gotocompany.depot.bigquery.exception.BQTableUpdateFailure;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoField;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link DepotStencilUpdateListener} that keeps a BigQuery table in sync with a protobuf schema.
 *
 * <p>Used when the sink consumes protobuf messages. Whenever the stencil schema cache is
 * refreshed, the listener regenerates the BigQuery schema from the configured proto
 * message (or key) class, appends the configured metadata columns, upserts the table, and
 * refreshes the cached
 * {@link com.gotocompany.depot.bigquery.converter.MessageRecordConverter} so that
 * subsequent messages are converted using the latest schema.</p>
 *
 * @see BigqueryFields
 * @see com.gotocompany.depot.bigquery.BigqueryStencilUpdateListenerFactory
 */
@Slf4j
public class BigqueryProtoUpdateListener extends DepotStencilUpdateListener {
    /** Sink configuration providing schema classes, metadata settings and the namespace. */
    private final BigQuerySinkConfig config;
    /** Client used to upsert the regenerated BigQuery table schema. */
    private final BigQueryClient bqClient;
    /** Cache that exposes and stores the current message-to-record converter. */
    @Getter
    private final MessageRecordConverterCache converterCache;

    /**
     * Creates the listener with its configuration, BigQuery client and converter cache.
     *
     * @param config         the sink configuration providing the proto schema classes and
     *                       metadata settings
     * @param bqClient       the client used to upsert the table schema
     * @param converterCache the cache that holds the converter refreshed on each schema
     *                       update
     */
    public BigqueryProtoUpdateListener(BigQuerySinkConfig config, BigQueryClient bqClient, MessageRecordConverterCache converterCache) {
        this.config = config;
        this.bqClient = bqClient;
        this.converterCache = converterCache;
    }

    /**
     * Regenerates and applies the BigQuery schema when the stencil cache changes.
     *
     * <p>Selects the proto schema class according to the configured message mode, obtains
     * the corresponding {@link ProtoField} (using the supplied descriptors when present, or
     * resolving it from the parser otherwise), generates the BigQuery fields, appends the
     * configured metadata fields, upserts the table, and installs a fresh
     * {@link com.gotocompany.depot.bigquery.converter.MessageRecordConverter} in the
     * cache.</p>
     *
     * @param newDescriptors the freshly refreshed descriptors keyed by class name, or
     *                       {@code null} to resolve the schema from the parser
     * @throws com.gotocompany.depot.bigquery.exception.BQTableUpdateFailure if regenerating
     *                       or upserting the table fails because of a BigQuery or I/O error
     * @throws com.gotocompany.depot.bigquery.exception.BQSchemaMappingException if a metadata
     *                       field collides with a field already present in the schema
     */
    @Override
    public void onSchemaUpdate(Map<String, Descriptor> newDescriptors) {
        log.info("stencil cache was refreshed, validating if bigquery schema changed");
        try {
            SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
            String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                    ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();
            ProtoMessageParser messageParser = (ProtoMessageParser) getMessageParser();
            ProtoField protoField;
            if (newDescriptors == null) {
                protoField = messageParser.getProtoField(schemaClass);
            } else {
                protoField = messageParser.getProtoField(schemaClass, newDescriptors);
            }
            List<Field> bqSchemaFields = BigqueryFields.generateBigquerySchema(protoField);
            addMetadataFields(bqSchemaFields);
            bqClient.upsertTable(bqSchemaFields);
            converterCache.setMessageRecordConverter(new MessageRecordConverter(messageParser, config));
        } catch (BigQueryException | IOException e) {
            String errMsg = "Error while updating bigquery table on callback:" + e.getMessage();
            log.error(errMsg);
            throw new BQTableUpdateFailure(errMsg, e);
        }
    }

    /**
     * Triggers an initial schema synchronisation.
     *
     * <p>Delegates to {@link #onSchemaUpdate(Map)} with {@code null} descriptors, causing the
     * schema to be resolved from the message parser.</p>
     *
     * @throws com.gotocompany.depot.bigquery.exception.BQTableUpdateFailure if updating the
     *                       BigQuery table fails
     * @throws com.gotocompany.depot.bigquery.exception.BQSchemaMappingException if a metadata
     *                       field collides with a field already present in the schema
     */
    @Override
    public void updateSchema() {
        onSchemaUpdate(null);
    }

    /**
     * Appends the configured metadata fields to the generated schema fields.
     *
     * <p>When metadata is enabled, adds either the flat metadata fields or, when a namespace
     * is configured, a single namespaced record field. Before appending, verifies that none
     * of the metadata field names already exist among the generated schema fields.</p>
     *
     * @param bqSchemaFields the mutable list of generated schema fields to which metadata
     *                       fields are appended
     * @throws BQSchemaMappingException if one or more metadata fields are already present in
     *                                  the schema
     */
    private void addMetadataFields(List<Field> bqSchemaFields) {
        List<Field> bqMetadataFields = new ArrayList<>();
        String namespaceName = config.getBqMetadataNamespace();
        if (config.shouldAddMetadata()) {
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            if (namespaceName.isEmpty()) {
                bqMetadataFields.addAll(BigqueryFields.getMetadataFields(metadataColumnsTypes));
            } else {
                bqMetadataFields.add(BigqueryFields.getNamespacedMetadataField(namespaceName, metadataColumnsTypes));
            }
        }

        List<String> duplicateFields = getDuplicateFields(bqSchemaFields, bqMetadataFields).stream().map(Field::getName).collect(Collectors.toList());
        if (duplicateFields.size() > 0) {
            throw new BQSchemaMappingException(String.format("Metadata field(s) is already present in the schema. "
                    + "fields: %s", duplicateFields));
        }
        bqSchemaFields.addAll(bqMetadataFields);
    }

    /**
     * Releases resources held by this listener.
     *
     * <p>This implementation holds no closeable resources and therefore does nothing.</p>
     *
     * @throws IOException declared for symmetry with closeable listeners; never thrown by
     *                     this implementation
     */
    public void close() throws IOException {
    }

    /**
     * Returns the fields from the first list whose names also occur in the second list.
     *
     * @param fields1 the fields to test
     * @param fields2 the fields whose names define the set of duplicates
     * @return the elements of {@code fields1} whose name is present in {@code fields2}
     */
    private List<Field> getDuplicateFields(List<Field> fields1, List<Field> fields2) {
        return fields1.stream().filter(field -> containsField(fields2, field.getName())).collect(Collectors.toList());
    }

    /**
     * Returns whether a field with the given name exists in the list.
     *
     * @param fields    the fields to search
     * @param fieldName the field name to look for
     * @return {@code true} if any field in {@code fields} has the given name, {@code false}
     *         otherwise
     */
    private boolean containsField(List<Field> fields, String fieldName) {
        return fields.stream().anyMatch(field -> field.getName().equals(fieldName));
    }

}
