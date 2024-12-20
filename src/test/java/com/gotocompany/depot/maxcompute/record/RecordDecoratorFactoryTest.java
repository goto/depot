package com.gotocompany.depot.maxcompute.record;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimeUnitType;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class RecordDecoratorFactoryTest {

    @Test
    public void shouldCreateDataRecordDecorator() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getMaxComputeTimeUnitType()).thenReturn(MaxComputeTimeUnitType.TIMESTAMP_NTZ);
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

    @Test
    public void shouldCreateDataRecordDecoratorWithNamespaceDecorator() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getMaxComputeTimeUnitType()).thenReturn(MaxComputeTimeUnitType.TIMESTAMP_NTZ);
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
