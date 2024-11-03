package com.gotocompany.depot.maxcompute.client.insertManager;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Manages partitioned inserts into MaxCompute tables using TableTunnel.
 * Handles grouping records by partition and streaming them to the appropriate partition.
 */
@RequiredArgsConstructor
@Slf4j
public class PartitionedInsertManager implements InsertManager {
    private final MaxComputeSinkConfig maxComputeConfig;
    private final TableTunnel tableTunnel;

    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        Map<String, List<RecordWrapper>> recordsByPartition = groupRecordsByPartition(recordWrappers);
        insertPartitionedRecords(recordsByPartition);
    }

    /**
     * Groups records by their partition specification.
     *
     * @param recordWrappers list of records to group
     * @return map of partition spec string to list of records
     */

    private Map<String, List<RecordWrapper>> groupRecordsByPartition(List<RecordWrapper> recordWrappers) {
        return recordWrappers.stream()
                .collect(Collectors.groupingBy(record -> record.getPartitionSpec().toString()));
    }

    /**
     * Inserts records into their respective partitions.
     *
     * @param recordsByPartition grouped records by partition
     * @throws TunnelException if tunnel operations fail
     * @throws IOException if I/O operations fail
     */

    private void insertPartitionedRecords(Map<String, List<RecordWrapper>> recordsByPartition)
            throws TunnelException, IOException {
        for (Map.Entry<String, List<RecordWrapper>> partitionEntry : recordsByPartition.entrySet()) {
            List<RecordWrapper> partitionRecords = partitionEntry.getValue();
            PartitionSpec partitionSpec = partitionRecords.get(0).getPartitionSpec();

            uploadRecordsToPartition(partitionSpec, partitionRecords, partitionEntry.getKey());
        }
    }

    /**
     * Uploads a list of records to a specific partition.
     *
     * @param partitionSpec partition specification
     * @param records records to upload
     * @param partitionKey partition identifier for logging
     * @throws TunnelException if tunnel operations fail
     * @throws IOException if I/O operations fail
     */

    private void uploadRecordsToPartition(
            PartitionSpec partitionSpec,
            List<RecordWrapper> records,
            String partitionKey) throws TunnelException, IOException {

        TableTunnel.StreamUploadSession uploadSession = createUploadSession(partitionSpec);
        TableTunnel.StreamRecordPack recordPack = uploadSession.newRecordPack();

        for (RecordWrapper record : records) {
            recordPack.append(record.getRecord());
        }

        flushRecords(recordPack, partitionKey);
    }

    /**
     * Creates a stream upload session for a partition.
     *
     * @param partitionSpec partition specification
     * @return configured upload session
     * @throws TunnelException if session creation fails
     */
    private TableTunnel.StreamUploadSession createUploadSession(PartitionSpec partitionSpec)
            throws TunnelException {
        return tableTunnel.buildStreamUploadSession(
                        maxComputeConfig.getMaxComputeProjectId(),
                        maxComputeConfig.getMaxComputeTableName())
                .setCreatePartition(true)
                .setPartitionSpec(partitionSpec)
                .build();
    }

    /**
     * Flushes records to MaxCompute and logs the result.
     *
     * @param recordPack pack of records to flush
     * @param partitionKey partition identifier for logging
     * @throws IOException if flush operation fails
     */
    private void flushRecords(TableTunnel.StreamRecordPack recordPack, String partitionKey)
            throws IOException {
        TableTunnel.FlushOption flushOption = new TableTunnel.FlushOption()
                .timeout(maxComputeConfig.getMaxComputeRecordPackFlushTimeout());

        TableTunnel.FlushResult flushResult = recordPack.flush(flushOption);

        log.info("Flushed {} records to partition {}", flushResult.getRecordCount(), partitionKey);
    }
}