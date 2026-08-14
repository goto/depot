package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link InsertManagerFactory}.
 *
 * <p>These tests verify that the factory selects the correct {@link InsertManager} implementation based solely
 * on the table-partitioning flag exposed by {@link MaxComputeSinkConfig}. The configuration is supplied as a
 * Mockito mock so that {@link MaxComputeSinkConfig#isTablePartitioningEnabled()} can be stubbed independently
 * per scenario, while the remaining collaborators ({@link TableTunnel}, {@link Instrumentation}, and
 * {@link MaxComputeMetrics}) are passed as bare mocks because the factory merely forwards them to the manager
 * it constructs.</p>
 *
 * @see InsertManagerFactory
 */
public class InsertManagerFactoryTest {

    /**
     * Verifies that a {@link PartitionedInsertManager} is created when table partitioning is enabled.
     *
     * <p>Given a {@link MaxComputeSinkConfig} mock whose
     * {@link MaxComputeSinkConfig#isTablePartitioningEnabled()} returns {@code true}, when
     * {@link InsertManagerFactory#createInsertManager(MaxComputeSinkConfig, TableTunnel, Instrumentation, MaxComputeMetrics)}
     * is invoked with mocked collaborators, then the returned {@link InsertManager} is asserted to be an
     * instance of {@link PartitionedInsertManager}.</p>
     */
    @Test
    public void shouldCreatePartitionedInsertManager() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);

        InsertManager insertManager = InsertManagerFactory.createInsertManager(maxComputeSinkConfig,
                Mockito.mock(TableTunnel.class), Mockito.mock(Instrumentation.class), Mockito.mock(MaxComputeMetrics.class));

        assertTrue(insertManager instanceof PartitionedInsertManager);
    }

    /**
     * Verifies that a {@link NonPartitionedInsertManager} is created when table partitioning is disabled.
     *
     * <p>Given a {@link MaxComputeSinkConfig} mock whose
     * {@link MaxComputeSinkConfig#isTablePartitioningEnabled()} returns {@code false}, when
     * {@link InsertManagerFactory#createInsertManager(MaxComputeSinkConfig, TableTunnel, Instrumentation, MaxComputeMetrics)}
     * is invoked with mocked collaborators, then the returned {@link InsertManager} is asserted to be an
     * instance of {@link NonPartitionedInsertManager}.</p>
     */
    @Test
    public void shouldCreateNonPartitionedInsertManager() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(false);

        InsertManager insertManager = InsertManagerFactory.createInsertManager(maxComputeSinkConfig,
                Mockito.mock(TableTunnel.class), Mockito.mock(Instrumentation.class), Mockito.mock(MaxComputeMetrics.class));

        assertTrue(insertManager instanceof NonPartitionedInsertManager);
    }

}
