package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

@RequiredArgsConstructor
public class TimestampPartitioningStrategy implements PartitioningStrategy {

    private final MaxComputeSinkConfig maxComputeSinkConfig;

    @Override
    public String getOriginalPartitionColumnName() {
        return maxComputeSinkConfig.getTablePartitionKey();
    }

    @Override
    public boolean shouldReplaceOriginalColumn() {
        return false;
    }

    @Override
    public Column getPartitionColumn() {
        return Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), TypeInfoFactory.STRING)
                .build();
    }

    @Override
    public PartitionSpec getPartitionSpec(Object object) {
        Timestamp timestamp = (Timestamp) object;
        long seconds = timestamp.getSeconds();
        int nanos = timestamp.getNanos();
        return new PartitionSpec(String.format("%s=%d", maxComputeSinkConfig.getTablePartitionColumnName(), getStartOfDayEpoch(seconds, nanos)));
    }

    private long getStartOfDayEpoch(long seconds, int nanos) {
        Instant instant = Instant.ofEpochSecond(seconds, nanos);
        ZoneId zoneId = ZoneId.of(maxComputeSinkConfig.getTablePartitionByTimestampTimezone());
        ZoneOffset zoneOffset = ZoneOffset.of(maxComputeSinkConfig.getTablePartitionByTimestampZoneOffset());
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zoneId);
        LocalDateTime startOfDay = localDateTime.toLocalDate().atStartOfDay();
        return startOfDay.toInstant(zoneOffset).getEpochSecond();
    }

}
