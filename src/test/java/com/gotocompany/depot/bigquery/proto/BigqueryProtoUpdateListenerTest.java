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

@RunWith(MockitoJUnitRunner.class)
public class BigqueryProtoUpdateListenerTest {
    @Mock
    private BigQueryClient bigQueryClient;
    @Mock
    private StencilClient stencilClient;

    private BigQuerySinkConfig config;
    private Configuration jsonPathConfig;

    private MessageRecordConverterCache converterWrapper;

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
        Configuration jsonPathConfig = Configuration.builder()
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
