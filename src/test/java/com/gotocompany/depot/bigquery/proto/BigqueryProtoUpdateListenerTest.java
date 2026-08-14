package com.gotocompany.depot.bigquery.proto;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.depot.TestKeyBQ;
import com.gotocompany.depot.message.proto.ProtoJsonProvider;
import com.gotocompany.depot.message.proto.TestProtoUtil;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.proto.ProtoField;
import com.gotocompany.depot.bigquery.models.Records;
import com.gotocompany.stencil.client.StencilClient;
import com.jayway.jsonpath.Configuration;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link BigqueryProtoUpdateListener}, which rebuilds the message converter and upserts
 * the BigQuery table whenever the stencil-managed protobuf schema changes.
 *
 * <p>The listener is built from a {@link BigQuerySinkConfig} (derived from system properties), a
 * mocked {@link BigQueryClient}, a mocked {@link StencilClient} and a real
 * {@link MessageRecordConverterCache}. The tests invoke {@code onSchemaUpdate} and then convert a
 * {@link Message} through the refreshed converter to assert that the new schema is applied; they also
 * verify metadata namespacing behaviour and that parser, converter and namespace-collision failures
 * propagate as runtime exceptions. The {@link MockitoJUnitRunner} initializes the
 * {@link Mock}-annotated collaborators.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class BigqueryProtoUpdateListenerTest {
    /** Mocked BigQuery client whose table upserts are verified. */
    @Mock
    private BigQueryClient bigQueryClient;
    /** Mocked stencil client supplying protobuf descriptors. */
    @Mock
    private StencilClient stencilClient;

    /** Sink configuration derived from system properties in {@link #setUp()}. */
    private BigQuerySinkConfig config;
    /** JSON-path configuration backing the protobuf message parser. */
    private Configuration jsonPathConfig;

    /** Real converter cache that holds the converter rebuilt on each schema update. */
    private MessageRecordConverterCache converterWrapper;

    /**
     * Sets the proto and metadata system properties and builds the shared fixtures before each test.
     *
     * @throws InvalidProtocolBufferException if stubbing the stencil client fails to parse a
     *                                        descriptor
     */
    @Before
    public void setUp() throws InvalidProtocolBufferException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestKeyBQ");
        System.setProperty("SINK_BIGQUERY_ENABLE_AUTO_SCHEMA_UPDATE", "false");
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "topic=string,partition=integer,offset=integer");
        config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(config))
                .build();
        converterWrapper = new MessageRecordConverterCache();
        when(stencilClient.parse(Mockito.anyString(), Mockito.any())).thenCallRealMethod();

    }

    /**
     * Verifies that a schema update rebuilds the converter to honour the new proto schema.
     *
     * <p>Given a descriptor map for {@link TestKeyBQ}, when {@code onSchemaUpdate} runs and a matching
     * message is converted, then it yields a single valid record whose {@code order_number} and
     * {@code order_url} columns reflect the message.</p>
     */
    @Test
    public void shouldUseNewSchemaIfProtoChanges() {
        BigqueryProtoUpdateListener bigqueryProtoUpdateListener = new BigqueryProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_url", 2));

        HashMap<String, Descriptor> descriptorsMap = new HashMap<String, Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), TestKeyBQ.getDescriptor());
        }};
        when(stencilClient.get(TestKeyBQ.class.getName())).thenReturn(descriptorsMap.get(TestKeyBQ.class.getName()));
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(BigqueryFields.getMetadataFields(new ArrayList<TupleString>() {{
                add(new TupleString("topic", "string"));
                add(new TupleString("partition", "integer"));
                add(new TupleString("offset", "integer"));
            }}));
        }};
        doNothing().when(bigQueryClient).upsertTable(bqSchemaFields);

        MessageParser parser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        bigqueryProtoUpdateListener.setMessageParser(parser);
        bigqueryProtoUpdateListener.onSchemaUpdate(descriptorsMap);
        TestKeyBQ testKeyBQ = TestKeyBQ.newBuilder().setOrderNumber("order").setOrderUrl("test").build();
        Message testMessage = new Message(
                "".getBytes(),
                testKeyBQ.toByteArray(),
                new Tuple<>("topic", "topic"),
                new Tuple<>("partition", 1),
                new Tuple<>("offset", 1));
        Records convert = bigqueryProtoUpdateListener.getConverterCache().getMessageRecordConverter().convert(Collections.singletonList(testMessage));
        Assert.assertEquals(1, convert.getValidRecords().size());
        Assert.assertEquals("order", convert.getValidRecords().get(0).getColumns().get("order_number"));
        Assert.assertEquals("test", convert.getValidRecords().get(0).getColumns().get("order_url"));
    }

    /**
     * Verifies that a {@code null} descriptor map falls back to the stencil client's descriptors.
     *
     * <p>Given the stencil client stubbed to return all descriptors, when {@code onSchemaUpdate} is
     * called with {@code null} and a matching message is converted, then it yields a single valid
     * record with the expected {@code order_number} and {@code order_url} columns.</p>
     */
    @Test
    public void shouldUseNewSchemaIfProtoChangesWhenNullDescriptorMapSentInBqSinkFactoryInit() {
        BigqueryProtoUpdateListener bigqueryProtoUpdateListener = new BigqueryProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_url", 2));

        HashMap<String, Descriptor> descriptorsMap = new HashMap<String, Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), TestKeyBQ.getDescriptor());
        }};
        when(stencilClient.get(TestKeyBQ.class.getName())).thenReturn(descriptorsMap.get(TestKeyBQ.class.getName()));
        when(stencilClient.getAll()).thenReturn(descriptorsMap);
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(BigqueryFields.getMetadataFields(new ArrayList<TupleString>() {{
                add(new TupleString("topic", "string"));
                add(new TupleString("partition", "integer"));
                add(new TupleString("offset", "integer"));
            }}));
        }};
        doNothing().when(bigQueryClient).upsertTable(bqSchemaFields);

        MessageParser parser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        bigqueryProtoUpdateListener.setMessageParser(parser);
        bigqueryProtoUpdateListener.onSchemaUpdate(null);
        TestKeyBQ testKeyBQ = TestKeyBQ.newBuilder().setOrderNumber("order").setOrderUrl("test").build();
        Message testMessage = new Message(
                "".getBytes(),
                testKeyBQ.toByteArray(),
                new Tuple<>("topic", "topic"),
                new Tuple<>("partition", 1),
                new Tuple<>("offset", 1));
        Records convert = bigqueryProtoUpdateListener.getConverterCache().getMessageRecordConverter().convert(Collections.singletonList(testMessage));
        Assert.assertEquals(1, convert.getValidRecords().size());
        Assert.assertEquals("order", convert.getValidRecords().get(0).getColumns().get("order_number"));
        Assert.assertEquals("test", convert.getValidRecords().get(0).getColumns().get("order_url"));
    }


    /**
     * Verifies that a schema update with a null descriptor raises a runtime exception.
     *
     * <p>Given a descriptor map whose value for the message class is {@code null}, when
     * {@code onSchemaUpdate} runs, then a {@link RuntimeException} is thrown.</p>
     */
    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfParserFails() {
        BigqueryProtoUpdateListener bigqueryProtoUpdateListener = new BigqueryProtoUpdateListener(config, bigQueryClient, converterWrapper);

        HashMap<String, Descriptor> descriptorsMap = new HashMap<String, Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), null);
        }};
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        bigqueryProtoUpdateListener.onSchemaUpdate(descriptorsMap);
    }

    /**
     * Verifies that a schema update fails when no message parser has been configured.
     *
     * <p>Given a valid descriptor map but no message parser set on the listener, when
     * {@code onSchemaUpdate} runs, then a {@link RuntimeException} is thrown.</p>
     */
    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfConverterFails() {
        BigqueryProtoUpdateListener bigqueryProtoUpdateListener = new BigqueryProtoUpdateListener(config, bigQueryClient, converterWrapper);
        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_url", 2));

        HashMap<String, Descriptor> descriptorsMap = new HashMap<String, Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), TestKeyBQ.getDescriptor());
        }};
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        bigqueryProtoUpdateListener.onSchemaUpdate(descriptorsMap);
    }

    /**
     * Verifies that {@code onSchemaUpdate} raises a runtime exception in this configuration.
     *
     * <p>Despite its name, the test does not simulate a dataset-location change: it supplies a valid
     * descriptor map but configures no message parser, so when {@code onSchemaUpdate} runs, then a
     * {@link RuntimeException} is thrown.</p>
     *
     * @throws IOException if descriptor handling fails
     */
    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDatasetLocationIsChanged() throws IOException {
        BigqueryProtoUpdateListener bigqueryProtoUpdateListener = new BigqueryProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_url", 2));

        HashMap<String, Descriptor> descriptorsMap = new HashMap<String, Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), TestKeyBQ.getDescriptor());
        }};
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        bigqueryProtoUpdateListener.onSchemaUpdate(descriptorsMap);
    }

    /**
     * Verifies that metadata fields are not namespaced when no namespace is configured.
     *
     * <p>Given an empty metadata namespace, when {@code onSchemaUpdate} runs, then the table is upserted
     * with the order fields followed by the flat (non-namespaced) metadata fields, and a converted
     * message yields the expected columns.</p>
     *
     * @throws IOException if descriptor handling fails
     */
    @Test
    public void shouldNotNamespaceMetadataFieldsWhenNamespaceIsNotProvided() throws IOException {
        BigqueryProtoUpdateListener bigqueryProtoUpdateListener = new BigqueryProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_url", 2));

        HashMap<String, Descriptor> descriptorsMap = new HashMap<String, Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), TestKeyBQ.getDescriptor());
        }};
        when(stencilClient.get(TestKeyBQ.class.getName())).thenReturn(descriptorsMap.get(TestKeyBQ.class.getName()));
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(BigqueryFields.getMetadataFields(new ArrayList<TupleString>() {{
                add(new TupleString("topic", "string"));
                add(new TupleString("partition", "integer"));
                add(new TupleString("offset", "integer"));
            }}));
        }};
        doNothing().when(bigQueryClient).upsertTable(bqSchemaFields);
        MessageParser parser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        bigqueryProtoUpdateListener.setMessageParser(parser);
        bigqueryProtoUpdateListener.onSchemaUpdate(descriptorsMap);
        TestKeyBQ testKeyBQ = TestKeyBQ.newBuilder().setOrderNumber("order").setOrderUrl("test").build();
        Message testMessage = new Message(
                "".getBytes(),
                testKeyBQ.toByteArray(),
                new Tuple<>("topic", "topic"),
                new Tuple<>("partition", 1),
                new Tuple<>("offset", 1));
        Records convert = bigqueryProtoUpdateListener.getConverterCache().getMessageRecordConverter().convert(Collections.singletonList(testMessage));
        Assert.assertEquals(1, convert.getValidRecords().size());
        Assert.assertEquals("order", convert.getValidRecords().get(0).getColumns().get("order_number"));
        Assert.assertEquals("test", convert.getValidRecords().get(0).getColumns().get("order_url"));
        verify(bigQueryClient, times(1)).upsertTable(bqSchemaFields); // assert that metadata fields were not namespaced
    }

    /**
     * Verifies that metadata fields are grouped under the configured namespace.
     *
     * <p>Given a {@code metadata_ns} namespace, when {@code onSchemaUpdate} runs, then the table is
     * upserted with the order fields plus a single namespaced metadata record field, and a converted
     * message yields the expected columns.</p>
     *
     * @throws IOException if descriptor handling fails
     */
    @Test
    public void shouldNamespaceMetadataFieldsWhenNamespaceIsProvided() throws IOException {
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "metadata_ns");
        config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        BigqueryProtoUpdateListener bigqueryProtoUpdateListener = new BigqueryProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_url", 2));

        HashMap<String, Descriptor> descriptorsMap = new HashMap<String, Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), TestKeyBQ.getDescriptor());
        }};
        when(stencilClient.get(TestKeyBQ.class.getName())).thenReturn(descriptorsMap.get(TestKeyBQ.class.getName()));
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(BigqueryFields.getNamespacedMetadataField(config.getBqMetadataNamespace(), new ArrayList<TupleString>() {{
                add(new TupleString("topic", "string"));
                add(new TupleString("partition", "integer"));
                add(new TupleString("offset", "integer"));
            }}));
        }};
        doNothing().when(bigQueryClient).upsertTable(bqSchemaFields);

        MessageParser parser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        bigqueryProtoUpdateListener.setMessageParser(parser);
        bigqueryProtoUpdateListener.onSchemaUpdate(descriptorsMap);
        TestKeyBQ testKeyBQ = TestKeyBQ.newBuilder().setOrderNumber("order").setOrderUrl("test").build();
        Message testMessage = new Message(
                "".getBytes(),
                testKeyBQ.toByteArray(),
                new Tuple<>("topic", "topic"),
                new Tuple<>("partition", 1),
                new Tuple<>("offset", 1));
        Records convert = bigqueryProtoUpdateListener.getConverterCache().getMessageRecordConverter().convert(Collections.singletonList(testMessage));
        Assert.assertEquals(1, convert.getValidRecords().size());
        Assert.assertEquals("order", convert.getValidRecords().get(0).getColumns().get("order_number"));
        Assert.assertEquals("test", convert.getValidRecords().get(0).getColumns().get("order_url"));

        verify(bigQueryClient, times(1)).upsertTable(bqSchemaFields);
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
    }

    /**
     * Verifies that a metadata namespace colliding with an existing column is rejected.
     *
     * <p>Given the metadata namespace set to {@code order_number} (an existing column), when
     * {@code onSchemaUpdate} runs, then a {@link RuntimeException} is thrown reporting the collision and
     * the table is never upserted.</p>
     *
     * @throws IOException if descriptor handling fails
     */
    @Test
    public void shouldThrowExceptionWhenMetadataNamespaceNameCollidesWithAnyFieldName() throws IOException {
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "order_number"); // set field name to an existing column name
        config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        BigqueryProtoUpdateListener bigqueryProtoUpdateListener = new BigqueryProtoUpdateListener(config, bigQueryClient, converterWrapper);

        ProtoField returnedProtoField = new ProtoField();
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_number", 1));
        returnedProtoField.addField(TestProtoUtil.createProtoField("order_url", 2));

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(BigqueryFields.getNamespacedMetadataField(config.getBqMetadataNamespace(), new ArrayList<TupleString>() {{
                add(new TupleString("topic", "string"));
                add(new TupleString("partition", "integer"));
                add(new TupleString("offset", "integer"));
            }}));
        }};

        HashMap<String, Descriptor> descriptorsMap = new HashMap<String, Descriptor>() {{
            put(String.format("%s", TestKeyBQ.class.getName()), TestKeyBQ.getDescriptor());
        }};
        jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(config))
                .build();
        MessageParser parser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        bigqueryProtoUpdateListener.setMessageParser(parser);

        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            bigqueryProtoUpdateListener.onSchemaUpdate(descriptorsMap);
        });
        Assert.assertEquals("Metadata field(s) is already present in the schema. fields: [order_number]", exception.getMessage());
        verify(bigQueryClient, times(0)).upsertTable(bqSchemaFields);
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
    }

}
