package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class MetadataUtilTest {

    private MetadataUtil metadataUtil;

    @Before
    public void setup() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        Mockito.when(maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()).thenReturn(false);
        this.metadataUtil = new MetadataUtil(maxComputeSinkConfig);
    }

    @Test
    public void shouldReturnAppropriateStructTypeInfoForNamespacedMetadata() {
        List<TupleString> metadataColumnTypes = Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                new TupleString("__kafka_topic", "string"),
                new TupleString("__kafka_offset", "long")
        );

        StructTypeInfo structTypeInfo = metadataUtil.getMetadataTypeInfo(metadataColumnTypes);

        assertThat(structTypeInfo.getFieldNames()).containsExactlyInAnyOrder("__message_timestamp", "__kafka_topic", "__kafka_offset");
        assertThat(structTypeInfo.getFieldTypeInfos()).containsExactlyInAnyOrder(
                TypeInfoFactory.TIMESTAMP_NTZ, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT
        );
    }

    @Test
    public void shouldReturnAppropriateTypeInfoForMetadataType() {
        assertThat(metadataUtil.getMetadataTypeInfo("integer")).isEqualTo(TypeInfoFactory.INT);
        assertThat(metadataUtil.getMetadataTypeInfo("long")).isEqualTo(TypeInfoFactory.BIGINT);
        assertThat(metadataUtil.getMetadataTypeInfo("float")).isEqualTo(TypeInfoFactory.FLOAT);
        assertThat(metadataUtil.getMetadataTypeInfo("double")).isEqualTo(TypeInfoFactory.DOUBLE);
        assertThat(metadataUtil.getMetadataTypeInfo("string")).isEqualTo(TypeInfoFactory.STRING);
        assertThat(metadataUtil.getMetadataTypeInfo("boolean")).isEqualTo(TypeInfoFactory.BOOLEAN);
        assertThat(metadataUtil.getMetadataTypeInfo("timestamp")).isEqualTo(TypeInfoFactory.TIMESTAMP_NTZ);
    }

    @Test
    public void shouldReturnTimestampWhenConfigured() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        this.metadataUtil = new MetadataUtil(maxComputeSinkConfig);

        assertThat(metadataUtil.getMetadataTypeInfo("timestamp")).isEqualTo(TypeInfoFactory.TIMESTAMP);
    }

    @Test
    public void shouldReturnAppropriateStructTypeInfoWithTimestampForNamespacedMetadataWhenConfigured() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        this.metadataUtil = new MetadataUtil(maxComputeSinkConfig);
        List<TupleString> metadataColumnTypes = Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                new TupleString("__kafka_topic", "string"),
                new TupleString("__kafka_offset", "long")
        );

        StructTypeInfo structTypeInfo = metadataUtil.getMetadataTypeInfo(metadataColumnTypes);

        assertThat(structTypeInfo.getFieldNames()).containsExactlyInAnyOrder("__message_timestamp", "__kafka_topic", "__kafka_offset");
        assertThat(structTypeInfo.getFieldTypeInfos()).containsExactlyInAnyOrder(
                TypeInfoFactory.TIMESTAMP, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT
        );
    }

    @Test
    public void shouldReturnIntegerValue() {
        assertThat(metadataUtil.getValidMetadataValue("integer", 1)).isEqualTo(1);
    }

    @Test
    public void shouldReturnLongValue() {
        assertThat(metadataUtil.getValidMetadataValue("long", 1L)).isEqualTo(1L);
    }

    @Test
    public void shouldReturnFloatValue() {
        assertThat(metadataUtil.getValidMetadataValue("float", 1.0f)).isEqualTo(1.0f);
    }

    @Test
    public void shouldReturnDoubleValue() {
        assertThat(metadataUtil.getValidMetadataValue("double", 1.0)).isEqualTo(1.0);
    }

    @Test
    public void shouldReturnStringValue() {
        assertThat(metadataUtil.getValidMetadataValue("string", "1")).isEqualTo("1");
    }

    @Test
    public void shouldReturnBooleanValue() {
        assertThat(metadataUtil.getValidMetadataValue("boolean", true)).isEqualTo(true);
    }

    @Test
    public void shouldReturnTimestampNtzValue() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        long epoch = 1734656400000L; // December 20th, 2024 01:00:00
        LocalDateTime expectedValue = LocalDateTime.of(2024, 12, 20, 1, 0, 0);

        assertThat(metadataUtil.getValidMetadataValue("timestamp", epoch)).isEqualTo(expectedValue);
    }

    @Test
    public void shouldReturnTimestampValue() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        this.metadataUtil = new MetadataUtil(maxComputeSinkConfig);
        long epoch = 1734656400000L; // December 20th, 2024 01:00:00
        LocalDateTime localDateTime = LocalDateTime.of(2024, 12, 20, 1, 0, 0);
        Timestamp expectedTimestamp = Timestamp.valueOf(localDateTime);

        assertThat(metadataUtil.getValidMetadataValue("timestamp", epoch)).isEqualTo(expectedTimestamp);
    }

}
