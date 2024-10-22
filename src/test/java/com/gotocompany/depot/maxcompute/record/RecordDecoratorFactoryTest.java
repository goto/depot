package com.gotocompany.depot.maxcompute.record;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

public class RecordDecoratorFactoryTest {

    @Test
    public void shouldCreateDataRecordDecorator() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        RecordDecoratorFactory recordDecoratorFactory = new RecordDecoratorFactory(maxComputeSinkConfig, null,
                null,
                null,
                null);

        RecordDecorator recordDecorator = recordDecoratorFactory.createRecordDecorator();

        Assertions.assertThat(recordDecorator)
                .isInstanceOf(ProtoDataColumnRecordDecorator.class)
                .extracting("decorator")
                .isNull();
    }

    @Test
    public void shouldCreateDataRecordDecoratorWithNamespaceDecorator() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        RecordDecoratorFactory recordDecoratorFactory = new RecordDecoratorFactory(maxComputeSinkConfig, null,
                null,
                null,
                null);

        RecordDecorator recordDecorator = recordDecoratorFactory.createRecordDecorator();

        Assertions.assertThat(recordDecorator)
                .isInstanceOf(ProtoMetadataColumnRecordDecorator.class)
                .extracting("decorator")
                .isNotNull()
                .isInstanceOf(ProtoDataColumnRecordDecorator.class);
    }
}
