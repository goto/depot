package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import dev.cel.common.CelValidationException;
import dev.cel.runtime.CelEvaluationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProtoMappingEngineTest {

    private static final String SOURCE_CLASS = "com.gotocompany.depot.kafka.test.KafkaSourceMessage";
    private static final String SINK_KEY_CLASS = "com.gotocompany.depot.kafka.test.KafkaSinkKey";
    private static final String SINK_MESSAGE_CLASS = "com.gotocompany.depot.kafka.test.KafkaSinkMessage";

    private StencilClient stencilClient;
    private Descriptors.Descriptor sourceDescriptor;
    private Descriptors.Descriptor sinkKeyDescriptor;
    private Descriptors.Descriptor sinkMessageDescriptor;

    @Before
    public void setUp() {
        stencilClient = StencilClientFactory.getClient();
        sourceDescriptor = stencilClient.get(SOURCE_CLASS);
        sinkKeyDescriptor = stencilClient.get(SINK_KEY_CLASS);
        sinkMessageDescriptor = stencilClient.get(SINK_MESSAGE_CLASS);
    }

    @Test
    public void shouldMapDirectFieldsTypeCastAndNestedAccess() throws Exception {
        String mapping = "{"
                + "\"order_id\": \"string(source.order_number)\","
                + "\"user_id\": \"source.account_go_id\","
                + "\"approval_status\": \"source.status.last_status\""
                + "}";

        ProtoMappingEngine engine = ProtoMappingEngine.create(
                mapping, sourceDescriptor, sinkKeyDescriptor, sinkMessageDescriptor);

        DynamicMessage source = buildSourceMessage(12345, "go-user-1", "approved");
        ProtoMappingEngine.MappedRecord result = engine.map(source);

        Assert.assertEquals("12345", result.getMessage().getField(
                sinkMessageDescriptor.findFieldByName("order_id")));
        Assert.assertEquals("go-user-1", result.getMessage().getField(
                sinkMessageDescriptor.findFieldByName("user_id")));
        Assert.assertEquals("approved", result.getMessage().getField(
                sinkMessageDescriptor.findFieldByName("approval_status")));
    }

    @Test
    public void shouldMapStaticConstantAndKeyField() throws Exception {
        String mapping = "{"
                + "\"key_order_id\": \"\\\"prefix\\\" + string(source.order_number)\","
                + "\"service_area_id\": \"34\""
                + "}";

        ProtoMappingEngine engine = ProtoMappingEngine.create(
                mapping, sourceDescriptor, sinkKeyDescriptor, sinkMessageDescriptor);

        DynamicMessage source = buildSourceMessage(99, "user-99", null);
        ProtoMappingEngine.MappedRecord result = engine.map(source);

        Assert.assertEquals("prefix99", result.getKey().getField(
                sinkKeyDescriptor.findFieldByName("key_order_id")));
        Assert.assertEquals(34, result.getMessage().getField(
                sinkMessageDescriptor.findFieldByName("service_area_id")));
    }

    @Test
    public void shouldMapTernaryExpression() throws Exception {
        String mapping = "{"
                + "\"key_order_id\": \"source.current_status == \\\"active\\\" ? \\\"running\\\" : \\\"stopped\\\"\""
                + "}";

        ProtoMappingEngine engine = ProtoMappingEngine.create(
                mapping, sourceDescriptor, sinkKeyDescriptor, sinkMessageDescriptor);

        DynamicMessage activeSource = buildSourceMessage(1, "u1", null, "active");
        Assert.assertEquals("running", engine.map(activeSource).getKey().getField(
                sinkKeyDescriptor.findFieldByName("key_order_id")));

        DynamicMessage inactiveSource = buildSourceMessage(1, "u1", null, "idle");
        Assert.assertEquals("stopped", engine.map(inactiveSource).getKey().getField(
                sinkKeyDescriptor.findFieldByName("key_order_id")));
    }

    @Test(expected = CelValidationException.class)
    public void shouldFailCompileAtInitForInvalidCel() throws Exception {
        ProtoMappingEngine.create(
                "{\"service_area_id\": \"source..invalid\"}",
                sourceDescriptor,
                sinkKeyDescriptor,
                sinkMessageDescriptor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailForUnknownOutputField() throws Exception {
        ProtoMappingEngine.create(
                "{\"not_a_field\": \"source.account_go_id\"}",
                sourceDescriptor,
                sinkKeyDescriptor,
                sinkMessageDescriptor);
    }

    private DynamicMessage buildSourceMessage(
            int orderNumber, String accountGoId, String lastStatus) {
        return buildSourceMessage(orderNumber, accountGoId, lastStatus, null);
    }

    private DynamicMessage buildSourceMessage(
            int orderNumber, String accountGoId, String lastStatus, String currentStatus) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(sourceDescriptor);
        builder.setField(sourceDescriptor.findFieldByName("order_number"), orderNumber);
        builder.setField(sourceDescriptor.findFieldByName("account_go_id"), accountGoId);
        if (lastStatus != null) {
            Descriptors.Descriptor statusDescriptor =
                    sourceDescriptor.findFieldByName("status").getMessageType();
            DynamicMessage status = DynamicMessage.newBuilder(statusDescriptor)
                    .setField(statusDescriptor.findFieldByName("last_status"), lastStatus)
                    .build();
            builder.setField(sourceDescriptor.findFieldByName("status"), status);
        }
        if (currentStatus != null) {
            builder.setField(sourceDescriptor.findFieldByName("current_status"), currentStatus);
        }
        return builder.build();
    }
}
