package com.gotocompany.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.StatusBQ;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.TestNestedRepeatedMessageBQ;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.message.proto.TestProtoUtil;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.Mockito.CALLS_REAL_METHODS;


public class BigQueryProtoPayloadConverterTest {


    private Descriptors.Descriptor testDescriptor;
    private BigQueryProtoPayloadConverter converter;
    private TableSchema testMessageBQSchema;
    private ProtoMessageParser protoMessageParser;

    @Before
    public void setUp() throws IOException, Descriptors.DescriptorValidationException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessageBQ");
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
        ClassLoadStencilClient stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        protoMessageParser = new ProtoMessageParser(stencilClient);
        testMessageBQSchema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("order_number")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("aliases")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("discount")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("order_url")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("price")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.DOUBLE)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("counter")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("status")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("trip_duration")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("seconds")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("nanos")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("current_state")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("key")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.STRING)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("value")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.STRING)
                                .build())
                        .build())
                .build();
        testDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(testMessageBQSchema);
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        BigQueryProtoWriter writer = Mockito.mock(BigQueryProtoWriter.class);
        converter = new BigQueryProtoPayloadConverter(config, protoMessageParser, writer);
        Mockito.when(writer.getDescriptor()).thenReturn(testDescriptor);
    }

    @Test
    public void shouldConvertPrimitiveFields() throws Exception {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setOrderNumber("order-no-112")
                .setOrderUrl("order-url-1")
                .setDiscount(1200L)
                .setPrice(23)
                .setCounter(20)
                .setStatus(StatusBQ.COMPLETED)
                .addAliases("alias1").addAliases("alias2")
                .build();
        DynamicMessage convertedMessage = converter.convert(new Message(null, m1.toByteArray()));
        Assert.assertEquals("order-no-112", convertedMessage.getField(testDescriptor.findFieldByName("order_number")));
        Assert.assertEquals("order-url-1", convertedMessage.getField(testDescriptor.findFieldByName("order_url")));
        Assert.assertEquals(1200L, convertedMessage.getField(testDescriptor.findFieldByName("discount")));
        List aliases = (List) convertedMessage.getField(testDescriptor.findFieldByName("aliases"));
        Assert.assertEquals("alias1", aliases.get(0));
        Assert.assertEquals("alias2", aliases.get(1));
        Assert.assertEquals(20L, convertedMessage.getField(testDescriptor.findFieldByName("counter")));
        Assert.assertEquals("COMPLETED", convertedMessage.getField(testDescriptor.findFieldByName("status")));
    }

    @Test
    public void shouldReturnDurationField() throws IOException {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setTripDuration(Duration.newBuilder().setSeconds(1234L).setNanos(231).build())
                .build();
        DynamicMessage convertedMessage = converter.convert(new Message(null, m1.toByteArray()));
        DynamicMessage tripDuration = ((DynamicMessage) convertedMessage.getField(testDescriptor.findFieldByName("trip_duration")));
        Assert.assertEquals(1234L, tripDuration.getField(tripDuration.getDescriptorForType().findFieldByName("seconds")));
        Assert.assertEquals(231L, tripDuration.getField(tripDuration.getDescriptorForType().findFieldByName("nanos")));
    }

    @Test
    public void shouldReturnMapField() throws Exception {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .putCurrentState("k4", "v4")
                .putCurrentState("k3", "v3")
                .putCurrentState("k1", "v1")
                .putCurrentState("k2", "v2")
                .build();
        DynamicMessage convertedMessage = converter.convert(new Message(null, m1.toByteArray()));
        List<Object> currentState = ((List<Object>) convertedMessage.getField(testDescriptor.findFieldByName("current_state")));
        List<Tuple<String, String>> actual = currentState.stream().map(o -> {
            Map<String, String> values = ((DynamicMessage) o).getAllFields().entrySet().stream().collect(
                    Collectors.toMap(s -> s.getKey().getName(), s -> s.getValue().toString())
            );
            return new Tuple<>(values.get("key"), values.get("value"));
        }).collect(Collectors.toList());
        actual.sort(Comparator.comparing(Tuple::getFirst));
        List<Tuple<String, String>> expected = new ArrayList<Tuple<String, String>>() {{
            add(new Tuple<>("k1", "v1"));
            add(new Tuple<>("k2", "v2"));
            add(new Tuple<>("k3", "v3"));
            add(new Tuple<>("k4", "v4"));
        }};
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldReturnComplexAndNestedType() throws Descriptors.DescriptorValidationException, IOException {
        TableSchema schema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("single_message")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addAllFields(testMessageBQSchema.getFieldsList())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("repeated_message")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addAllFields(testMessageBQSchema.getFieldsList())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("number_field")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("repeated_number_field")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .build();
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestNestedRepeatedMessageBQ");
        testDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        BigQueryProtoWriter writer = Mockito.mock(BigQueryProtoWriter.class);
        converter = new BigQueryProtoPayloadConverter(config, protoMessageParser, writer);
        Mockito.when(writer.getDescriptor()).thenReturn(testDescriptor);

        Instant now = Instant.now();
        TestMessageBQ singleMessage = TestProtoUtil.generateTestMessage(now);
        TestMessageBQ nested1 = TestProtoUtil.generateTestMessage(now);
        TestMessageBQ nested2 = TestProtoUtil.generateTestMessage(now);
        TestNestedRepeatedMessageBQ message = TestNestedRepeatedMessageBQ.newBuilder()
                .setNumberField(123)
                .setSingleMessage(singleMessage)
                .addRepeatedMessage(nested1)
                .addRepeatedMessage(nested2)
                .addRepeatedNumberField(11)
                .addRepeatedNumberField(12)
                .build();
        DynamicMessage convertedMessage = converter.convert(new Message(null, message.toByteArray()));
        DynamicMessage sm1 = (DynamicMessage) convertedMessage.getField(testDescriptor.findFieldByName("single_message"));
        Assert.assertEquals(singleMessage.getOrderNumber(), sm1.getField(sm1.getDescriptorForType().findFieldByName("order_number")));
        List<DynamicMessage> nestedMessage = (List) convertedMessage.getField(testDescriptor.findFieldByName("repeated_message"));
        Assert.assertEquals(2, nestedMessage.size());
        DynamicMessage nestedMessage1 = nestedMessage.get(0);
        DynamicMessage nestedMessage2 = nestedMessage.get(1);
        Assert.assertEquals(nested1.getOrderNumber(), nestedMessage1.getField(sm1.getDescriptorForType().findFieldByName("order_number")));
        Assert.assertEquals(nested2.getOrderNumber(), nestedMessage2.getField(sm1.getDescriptorForType().findFieldByName("order_number")));
    }
}
