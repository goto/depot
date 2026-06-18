package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.exception.MaxComputeTableOperationException;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Wrapper class that listens to schema updates and updates the MaxCompute table schema.
 * It also caches the MaxCompute schema.
 *
 * <p>As a {@link DepotStencilUpdateListener}, it is notified whenever the upstream Protobuf
 * descriptors change. On each update (or on first access) it rebuilds the schema from the relevant
 * descriptor via {@link MaxComputeSchemaBuilder}, upserts the table through the
 * {@link MaxComputeClient}, re-reads the authoritative server-side schema, and replaces the cached
 * schema. The cached value is the single source of truth for the table schema used when decorating
 * records.</p>
 *
 * <p>Schema updates are synchronized to keep the cache consistent across threads; the Protobuf
 * converter cache is cleared after every update so converters are rebuilt against the new schema.</p>
 */
@Slf4j
public class MaxComputeSchemaCache extends DepotStencilUpdateListener {

    /**
     * Builder used to construct a {@link MaxComputeSchema} from a Protobuf descriptor.
     */
    private final MaxComputeSchemaBuilder maxComputeSchemaBuilder;
    /**
     * Sink configuration used to determine which Protobuf schema class to track.
     */
    private final SinkConfig sinkConfig;
    /**
     * Converter orchestrator whose cache is cleared after each schema update.
     */
    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    /**
     * Client used to create or update the table and to read back the latest server-side schema.
     */
    private final MaxComputeClient maxComputeClient;
    /**
     * Cached resolved schema, lazily initialised and refreshed on each schema update; {@code null}
     * until first built.
     */
    private MaxComputeSchema maxComputeSchema;

    /**
     * Creates a schema cache wired with the collaborators needed to build, persist, and refresh the
     * MaxCompute table schema.
     *
     * @param maxComputeSchemaBuilder builder that constructs the schema from a Protobuf descriptor
     * @param sinkConfig sink configuration identifying the Protobuf schema class to track
     * @param protobufConverterOrchestrator converter orchestrator whose cache is cleared after updates
     * @param maxComputeClient client used to upsert the table and fetch the latest schema
     */
    public MaxComputeSchemaCache(MaxComputeSchemaBuilder maxComputeSchemaBuilder,
                                 SinkConfig sinkConfig,
                                 ProtobufConverterOrchestrator protobufConverterOrchestrator,
                                 MaxComputeClient maxComputeClient) {
        this.maxComputeSchemaBuilder = maxComputeSchemaBuilder;
        this.sinkConfig = sinkConfig;
        this.protobufConverterOrchestrator = protobufConverterOrchestrator;
        this.maxComputeClient = maxComputeClient;
    }

    /**
     * Get the MaxCompute schema. This schema is single source of truth for MaxCompute table schema.
     * It is updated whenever the protobuf schema is updated.
     *
     * <p>This schema is the single source of truth for the MaxCompute table schema and is refreshed
     * whenever the upstream Protobuf schema changes. The lazy initialisation is synchronized to ensure
     * only one thread builds the initial schema.</p>
     *
     * @return MaxComputeSchema
     */
    public MaxComputeSchema getMaxComputeSchema() {
        synchronized (this) {
            if (maxComputeSchema == null) {
                updateSchema();
            }
        }
        return maxComputeSchema;
    }

    /**
     * Update the MaxCompute table schema based on the new protobuf schema from stencil.
     *
     * <p>The descriptor for the configured schema class is selected from the supplied map and used to
     * refresh the cached schema. This method is synchronized to serialise concurrent updates.</p>
     *
     * @param newDescriptor new protobuf class descriptors
     */
    @Override
    public synchronized void onSchemaUpdate(Map<String, Descriptors.Descriptor> newDescriptor) {
        Descriptors.Descriptor descriptor = newDescriptor.get(getSchemaClass());
        updateMaxComputeTableSchema(descriptor);
    }

    /**
     * Update the MaxCompute table schema based on the protobuf schema fetched from message parser.
     *
     * <p>The descriptor map is read from the
     * {@link com.gotocompany.depot.message.proto.ProtoMessageParser} and the descriptor for the tracked
     * schema class is used to rebuild and upsert the schema. This method is synchronized to serialise
     * concurrent updates.</p>
     */
    @Override
    public synchronized void updateSchema() {
        Map<String, Descriptors.Descriptor> descriptorMap = ((ProtoMessageParser) getMessageParser()).getDescriptorMap();
        Descriptors.Descriptor descriptor = descriptorMap.get(getSchemaClass());
        updateMaxComputeTableSchema(descriptor);
    }

    /**
     * Rebuilds the schema from the given descriptor, upserts the table, and caches the authoritative
     * server-side schema.
     *
     * <p>A local schema is built from the descriptor and used to create or update the table via the
     * {@link MaxComputeClient}. The latest server-side table schema is then fetched and combined with
     * the locally computed metadata columns to form the new cached schema. The Protobuf converter cache
     * is always cleared afterwards, even if the update fails.</p>
     *
     * @param descriptor the Protobuf descriptor to build the table schema from
     * @throws MaxComputeTableOperationException if the table create or update fails with an
     *         {@code OdpsException}
     */
    private void updateMaxComputeTableSchema(Descriptors.Descriptor descriptor) {
        MaxComputeSchema localSchema = maxComputeSchemaBuilder.build(descriptor);
        try {
            log.info("Upserting MaxCompute table schema");
            maxComputeClient.createOrUpdateTable(localSchema.getTableSchema());
            log.info("MaxCompute table upserted successfully");
            TableSchema serverSideTableSchema = maxComputeClient.getLatestTableSchema();
            maxComputeSchema = new MaxComputeSchema(
                    serverSideTableSchema,
                    localSchema.getMetadataColumns()
            );
        } catch (OdpsException e) {
            throw new MaxComputeTableOperationException("Error while updating MaxCompute table", e);
        } finally {
            log.info("Clearing protobuf converter cache");
            protobufConverterOrchestrator.clearCache();
        }
    }

    /**
     * Resolves the fully-qualified Protobuf class name whose schema this cache tracks.
     *
     * <p>The log-message class is used in {@code LOG_MESSAGE} mode; otherwise the key class is used.</p>
     *
     * @return the configured Protobuf schema class name to track
     */
    private String getSchemaClass() {
        return sinkConfig.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
    }
}
