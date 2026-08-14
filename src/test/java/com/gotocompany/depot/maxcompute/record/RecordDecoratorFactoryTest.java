package com.gotocompany.depot.maxcompute.record;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RecordDecoratorFactory}.
 *
 * <p>These tests verify that the factory assembles the correct {@link RecordDecorator} chain based on whether
 * metadata columns are enabled. The configuration objects ({@link MaxComputeSinkConfig} and {@link SinkConfig})
 * are Mockito mocks, while the remaining collaborators carried by
 * {@link RecordDecoratorFactory.RecordDecoratorConfig} are passed as mocks or lightweight real helpers. The
 * resulting chain is inspected with AssertJ, including its private {@code decorator} field, to confirm both the
 * head type and the presence or absence of a nested decorator.</p>
 *
 * @see RecordDecoratorFactory
 */
public class RecordDecoratorFactoryTest {

    /**
     * Verifies that a bare data-column decorator is created when metadata columns are disabled.
     *
     * <p>Given a {@link MaxComputeSinkConfig} whose {@link MaxComputeSinkConfig#shouldAddMetadata()} returns
     * {@code false}, when
     * {@link RecordDecoratorFactory#createRecordDecorator(RecordDecoratorFactory.RecordDecoratorConfig)} is
     * invoked, then the returned decorator is asserted to be a {@link ProtoDataColumnRecordDecorator} whose
     * nested {@code decorator} field is {@code null}, confirming it terminates the chain.</p>
     */
    @Test
    public void shouldCreateDataRecordDecorator() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.message.Message");

        RecordDecorator recordDecorator = RecordDecoratorFactory.createRecordDecorator(
                new RecordDecoratorFactory.RecordDecoratorConfig(
                        null, null, null, null,
                        maxComputeSinkConfig, sinkConfig, Mockito.mock(StatsDReporter.class),
                        Mockito.mock(MaxComputeMetrics.class),
                        new MetadataUtil(maxComputeSinkConfig)
                )
        );

        assertThat(recordDecorator)
                .isInstanceOf(ProtoDataColumnRecordDecorator.class)
                .extracting("decorator")
                .isNull();
    }

    /**
     * Verifies that a metadata-column decorator wrapping a data-column decorator is created when metadata is
     * enabled.
     *
     * <p>Given a {@link MaxComputeSinkConfig} whose {@link MaxComputeSinkConfig#shouldAddMetadata()} returns
     * {@code true}, when
     * {@link RecordDecoratorFactory#createRecordDecorator(RecordDecoratorFactory.RecordDecoratorConfig)} is
     * invoked, then the returned decorator is asserted to be a {@link ProtoMetadataColumnRecordDecorator} whose
     * nested {@code decorator} field is a non-null {@link ProtoDataColumnRecordDecorator}.</p>
     */
    @Test
    public void shouldCreateDataRecordDecoratorWithNamespaceDecorator() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.message.Message");

        RecordDecorator recordDecorator = RecordDecoratorFactory.createRecordDecorator(
                new RecordDecoratorFactory.RecordDecoratorConfig(
                        null, null, null,
                        null, maxComputeSinkConfig, sinkConfig, Mockito.mock(StatsDReporter.class),
                        Mockito.mock(MaxComputeMetrics.class), new MetadataUtil(maxComputeSinkConfig)
                )
        );

        assertThat(recordDecorator)
                .isInstanceOf(ProtoMetadataColumnRecordDecorator.class)
                .extracting("decorator")
                .isNotNull()
                .isInstanceOf(ProtoDataColumnRecordDecorator.class);
    }
}
