package com.gotocompany.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.StatusBQ;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.CALLS_REAL_METHODS;


public class BigQueryProtoPayloadConverterTest {


    private Descriptors.Descriptor testDescriptor;
    private BigQueryProtoPayloadConverter converter;

    @Before
    public void setUp() throws IOException, Descriptors.DescriptorValidationException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessageBQ");
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
        ClassLoadStencilClient stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient);
        TableSchema schema = TableSchema.newBuilder()
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
                .build();
        testDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
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
    public void testDurationField() throws IOException {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setTripDuration(Duration.newBuilder().setSeconds(1234L).setNanos(231).build())
                .build();
        DynamicMessage convertedMessage = converter.convert(new Message(null, m1.toByteArray()));
        DynamicMessage tripDuration = ((DynamicMessage) convertedMessage.getField(testDescriptor.findFieldByName("trip_duration")));
        Assert.assertEquals(1234L, tripDuration.getField(tripDuration.getDescriptorForType().findFieldByName("seconds")));
        Assert.assertEquals(231L, tripDuration.getField(tripDuration.getDescriptorForType().findFieldByName("nanos")));
    }
}