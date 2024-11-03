package com.gotocompany.depot.maxcompute.client.insertManager;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;


/**
 * Manages non-partitioned inserts into MaxCompute tables using TableTunnel.
 * Handles bulk record insertion into a single table without partitioning.
 */
@Slf4j
@RequiredArgsConstructor
public class NonPartitionedInsertManager implements InsertManager {
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final TableTunnel tableTunnel;

    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        TableTunnel.StreamUploadSession uploadSession = createUploadSession();
        uploadRecords(uploadSession, recordWrappers);
    }
    /**
     * Creates a new stream upload session for the table.
     *
     * @return configured upload session
     * @throws TunnelException if session creation fails
     */
    private TableTunnel.StreamUploadSession createUploadSession() throws TunnelException {
        return tableTunnel.buildStreamUploadSession(
                maxComputeSinkConfig.getMaxComputeProjectId(),
                maxComputeSinkConfig.getMaxComputeTableName()
        ).build();
    }

    /**
     * Uploads records to MaxCompute using the provided upload session.
     *
     * @param uploadSession session for uploading records
     * @param recordWrappers list of records to upload
     * @throws IOException if record upload or flush fails
     */
    private void uploadRecords(TableTunnel.StreamUploadSession uploadSession, List<RecordWrapper> recordWrappers) throws IOException {
        TableTunnel.StreamRecordPack recordPack = uploadSession.newRecordPack();
        for (RecordWrapper recordWrapper : recordWrappers) {
            recordPack.append(recordWrapper.getRecord());
        }
        flushRecords(recordPack);
    }

    /**
     * Flushes records to MaxCompute and logs the result.
     *
     * @param recordPack pack of records to flush
     * @throws IOException if flush operation fails
     */
    private void flushRecords(TableTunnel.StreamRecordPack recordPack) throws IOException {
        TableTunnel.FlushOption flushOption = new TableTunnel.FlushOption()
                .timeout(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeout());

        TableTunnel.FlushResult flushResult = recordPack.flush(flushOption);
        log.info("Flushed {} records", flushResult.getRecordCount());
    }
}
