package com.gotocompany.depot.kafka.schema;

import com.gotocompany.depot.TestKafkaOutputKey;
import com.gotocompany.depot.TestKafkaOutputMessage;
import com.gotocompany.depot.TestKafkaSourceMessage;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunction;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunctionCache;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunctionFactory;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.KafkaSinkMetrics;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class KafkaSinkStencilUpdateListenerTest {

    private static final String SOURCE_CLASS = "com.gotocompany.depot.TestKafkaSourceMessage";
    private static final String VALUE_CLASS = "com.gotocompany.depot.TestKafkaOutputMessage";
    private static final String KEY_CLASS = "com.gotocompany.depot.TestKafkaOutputKey";

    @Mock
    private KafkaSinkConfig sinkConfig;
    @Mock
    private ProtoMessageParser protoMessageParser;
    @Mock
    private StencilClient sinkStencilClient;
    @Mock
    private KafkaSinkMetrics metrics;
    @Mock
    private Instrumentation instrumentation;
    private ProtoMappingFunctionCache mappingFunctionCache;
    private KafkaSinkStencilUpdateListener updateListener;

    @Before
    public void setup() {
        mappingFunctionCache = new ProtoMappingFunctionCache();
        updateListener = new KafkaSinkStencilUpdateListener(
                sinkConfig, new ProtoMappingFunctionFactory(), mappingFunctionCache, metrics, instrumentation);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn(SOURCE_CLASS);
        when(sinkConfig.getSinkKafkaProtoMessage()).thenReturn(VALUE_CLASS);
        when(sinkConfig.getSinkKafkaProtoKey()).thenReturn(KEY_CLASS);
        when(sinkConfig.getSinkKafkaProtoMapping()).thenReturn(Collections.singletonMap("order_id", "string(source.order_number)"));
        when(protoMessageParser.getDescriptor(SOURCE_CLASS)).thenReturn(TestKafkaSourceMessage.getDescriptor());
        when(sinkStencilClient.get(VALUE_CLASS)).thenReturn(TestKafkaOutputMessage.getDescriptor());
        when(sinkStencilClient.get(KEY_CLASS)).thenReturn(TestKafkaOutputKey.getDescriptor());
    }

    @Test
    public void shouldBuildMappingFunctionOnUpdateSchema() throws Exception {
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        updateListener.updateSchema();
        ProtoMappingFunction mappingFunction = mappingFunctionCache.get();
        assertTrue(mappingFunction.hasKeyMapping());
        TestKafkaSourceMessage sourceMessage = TestKafkaSourceMessage.newBuilder().setOrderNumber(7L).build();
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(sourceMessage).toByteArray());
        assertEquals("7", outputMessage.getOrderId());
    }

    @Test
    public void shouldBuildMappingFunctionOnSchemaUpdateCallback() {
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        updateListener.onSchemaUpdate(Collections.emptyMap());
        assertTrue(mappingFunctionCache.get().hasKeyMapping());
    }

    @Test
    public void shouldBuildMappingFunctionWithoutKeyMapperWhenKeyProtoIsNotConfigured() {
        when(sinkConfig.getSinkKafkaProtoKey()).thenReturn(null);
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        updateListener.updateSchema();
        assertFalse(mappingFunctionCache.get().hasKeyMapping());
    }

    @Test
    public void shouldSkipUpdateWhenMessageParserIsNotSet() {
        updateListener.setSinkStencilClient(sinkStencilClient);
        updateListener.updateSchema();
        assertThrows(IllegalStateException.class, mappingFunctionCache::get);
    }

    @Test
    public void shouldSkipUpdateWhenSinkStencilClientIsNotSet() {
        updateListener.setMessageParser(protoMessageParser);
        updateListener.updateSchema();
        assertThrows(IllegalStateException.class, mappingFunctionCache::get);
    }

    @Test
    public void shouldFailForNonProtoMessageParser() {
        updateListener.setMessageParser(Mockito.mock(MessageParser.class));
        updateListener.setSinkStencilClient(sinkStencilClient);
        ConfigurationException exception = assertThrows(ConfigurationException.class, () -> updateListener.updateSchema());
        assertEquals("kafka sink requires a protobuf message parser", exception.getMessage());
    }

    @Test
    public void shouldFailWhenSourceDescriptorIsNotFound() {
        when(protoMessageParser.getDescriptor(SOURCE_CLASS)).thenReturn(null);
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        ConfigurationException exception = assertThrows(ConfigurationException.class, () -> updateListener.updateSchema());
        assertTrue(exception.getMessage().contains("source proto class"));
    }

    @Test
    public void shouldFailWhenSinkValueDescriptorIsNotFound() {
        when(sinkStencilClient.get(VALUE_CLASS)).thenReturn(null);
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        ConfigurationException exception = assertThrows(ConfigurationException.class, () -> updateListener.updateSchema());
        assertTrue(exception.getMessage().contains("sink proto class"));
    }

    @Test
    public void shouldFailWhenSinkKeyDescriptorIsNotFound() {
        when(sinkStencilClient.get(KEY_CLASS)).thenReturn(null);
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        ConfigurationException exception = assertThrows(ConfigurationException.class, () -> updateListener.updateSchema());
        assertTrue(exception.getMessage().contains("sink proto class"));
    }

    @Test
    public void shouldGuardAgainstReentrantSchemaUpdates() {
        when(sinkStencilClient.get(VALUE_CLASS)).thenAnswer(invocation -> {
            updateListener.onSchemaUpdate(Collections.emptyMap());
            return TestKafkaOutputMessage.getDescriptor();
        });
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        updateListener.updateSchema();
        assertTrue(mappingFunctionCache.get().hasKeyMapping());
        Mockito.verify(sinkStencilClient, Mockito.times(1)).get(VALUE_CLASS);
    }

    @Test
    public void shouldRecoverAfterFailedUpdate() {
        when(sinkStencilClient.get(VALUE_CLASS)).thenReturn(null);
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        assertThrows(ConfigurationException.class, () -> updateListener.updateSchema());
        when(sinkStencilClient.get(VALUE_CLASS)).thenReturn(TestKafkaOutputMessage.getDescriptor());
        updateListener.updateSchema();
        assertTrue(mappingFunctionCache.get().hasKeyMapping());
    }

    @Test
    public void shouldRetainPreviousMappingFunctionWhenRebuildFails() {
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        updateListener.updateSchema();
        ProtoMappingFunction previousFunction = mappingFunctionCache.get();
        when(sinkConfig.getSinkKafkaProtoMapping())
                .thenReturn(Collections.singletonMap("unknown_field", "source.account_go_id"));
        assertThrows(ConfigurationException.class, () -> updateListener.updateSchema());
        Assert.assertSame(previousFunction, mappingFunctionCache.get());
    }

    @Test
    public void shouldCaptureSchemaUpdateSuccessMetricOnUpdate() {
        when(metrics.getKafkaSchemaUpdateTotalMetric()).thenReturn("schema_update_total");
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        updateListener.updateSchema();
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("schema_update_total", "state=success");
    }

    @Test
    public void shouldSwallowFailureAndRetainPreviousMappingWhenRefreshRebuildFails() {
        when(metrics.getKafkaSchemaUpdateTotalMetric()).thenReturn("schema_update_total");
        updateListener.setMessageParser(protoMessageParser);
        updateListener.setSinkStencilClient(sinkStencilClient);
        updateListener.updateSchema();
        ProtoMappingFunction previousFunction = mappingFunctionCache.get();
        when(sinkConfig.getSinkKafkaProtoMapping())
                .thenReturn(Collections.singletonMap("unknown_field", "source.account_go_id"));
        updateListener.onSchemaUpdate(Collections.emptyMap());
        Assert.assertSame(previousFunction, mappingFunctionCache.get());
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("schema_update_total", "state=failure");
    }
}
