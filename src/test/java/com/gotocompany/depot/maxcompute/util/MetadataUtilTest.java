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

/**
 * Unit tests for {@link MetadataUtil}.
 *
 * <p>These tests verify both halves of {@link MetadataUtil}: resolving Depot logical metadata type names to
 * MaxCompute {@link com.aliyun.odps.type.TypeInfo} values (individually and as a namespaced
 * {@link StructTypeInfo}), and coercing raw metadata values into their MaxCompute representations. A
 * {@link MaxComputeSinkConfig} mock controls the timestamp data type ({@code TIMESTAMP_NTZ} versus
 * {@code TIMESTAMP}), the {@link ZoneId}, and the integer-widening flag; several timestamp-specific tests build
 * their own {@link MetadataUtil} with a {@code TIMESTAMP} configuration.</p>
 *
 * @see MetadataUtil
 */
public class MetadataUtilTest {

    /**
     * The helper under test, initialised in {@link #setup()} with the default {@code TIMESTAMP_NTZ} fixture and
     * replaced in some tests with a {@code TIMESTAMP}-configured instance.
     */
    private MetadataUtil metadataUtil;

    /**
     * Builds the default {@link MetadataUtil} fixture before each test.
     *
     * <p>Mocks a {@link MaxComputeSinkConfig} that maps Protobuf timestamps to
     * {@link MaxComputeTimestampDataType#TIMESTAMP_NTZ}, uses the UTC zone, and disables integer widening to
     * {@code BIGINT}, then constructs the {@link MetadataUtil}.</p>
     */
    @Before
    public void setup() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        Mockito.when(maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()).thenReturn(false);
        this.metadataUtil = new MetadataUtil(maxComputeSinkConfig);
    }

    /**
     * Verifies that a list of metadata columns maps to a struct type with matching field names and types.
     *
     * <p>Given three metadata columns of logical types {@code timestamp}, {@code string}, and {@code long}, when
     * {@link MetadataUtil#getMetadataTypeInfo(List)} is called, then the resulting {@link StructTypeInfo} has the
     * field names {@code __message_timestamp}, {@code __kafka_topic}, and {@code __kafka_offset} and the field
     * types {@code TIMESTAMP_NTZ}, {@code STRING}, and {@code BIGINT}.</p>
     */
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

    /**
     * Verifies that each supported logical type name resolves to the expected MaxCompute type.
     *
     * <p>When {@link MetadataUtil#getMetadataTypeInfo(String)} is called for each logical name, then
     * {@code integer} maps to {@code INT}, {@code long} to {@code BIGINT}, {@code float} to {@code FLOAT},
     * {@code double} to {@code DOUBLE}, {@code string} to {@code STRING}, {@code boolean} to {@code BOOLEAN}, and
     * {@code timestamp} to {@code TIMESTAMP_NTZ} under the default fixture.</p>
     */
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

    /**
     * Verifies that the timestamp type resolves to {@code TIMESTAMP} when so configured.
     *
     * <p>Given a {@link MetadataUtil} built from a config that maps timestamps to
     * {@link MaxComputeTimestampDataType#TIMESTAMP}, when {@link MetadataUtil#getMetadataTypeInfo(String)} is
     * called for {@code timestamp}, then it returns {@code TIMESTAMP} rather than {@code TIMESTAMP_NTZ}.</p>
     */
    @Test
    public void shouldReturnTimestampWhenConfigured() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        this.metadataUtil = new MetadataUtil(maxComputeSinkConfig);

        assertThat(metadataUtil.getMetadataTypeInfo("timestamp")).isEqualTo(TypeInfoFactory.TIMESTAMP);
    }

    /**
     * Verifies that namespaced metadata uses {@code TIMESTAMP} for timestamp fields when so configured.
     *
     * <p>Given a {@link MetadataUtil} configured with {@link MaxComputeTimestampDataType#TIMESTAMP} and the same
     * three metadata columns, when {@link MetadataUtil#getMetadataTypeInfo(List)} is called, then the resulting
     * {@link StructTypeInfo} carries the field types {@code TIMESTAMP}, {@code STRING}, and {@code BIGINT}.</p>
     */
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

    /**
     * Verifies that an integer metadata value is returned as an {@code int} under the default fixture.
     *
     * <p>When {@link MetadataUtil#getValidMetadataValue(String, Object)} is called with type {@code integer} and
     * the value {@code 1}, then it returns the {@code int} {@code 1}, since integer widening to {@code BIGINT} is
     * disabled.</p>
     */
    @Test
    public void shouldReturnIntegerValue() {
        assertThat(metadataUtil.getValidMetadataValue("integer", 1)).isEqualTo(1);
    }

    /**
     * Verifies that a long metadata value is returned unchanged.
     *
     * <p>When {@link MetadataUtil#getValidMetadataValue(String, Object)} is called with type {@code long} and the
     * value {@code 1L}, then it returns the {@code long} {@code 1L}.</p>
     */
    @Test
    public void shouldReturnLongValue() {
        assertThat(metadataUtil.getValidMetadataValue("long", 1L)).isEqualTo(1L);
    }

    /**
     * Verifies that a float metadata value is returned unchanged.
     *
     * <p>When {@link MetadataUtil#getValidMetadataValue(String, Object)} is called with type {@code float} and
     * the value {@code 1.0f}, then it returns the {@code float} {@code 1.0f}.</p>
     */
    @Test
    public void shouldReturnFloatValue() {
        assertThat(metadataUtil.getValidMetadataValue("float", 1.0f)).isEqualTo(1.0f);
    }

    /**
     * Verifies that a double metadata value is returned unchanged.
     *
     * <p>When {@link MetadataUtil#getValidMetadataValue(String, Object)} is called with type {@code double} and
     * the value {@code 1.0}, then it returns the {@code double} {@code 1.0}.</p>
     */
    @Test
    public void shouldReturnDoubleValue() {
        assertThat(metadataUtil.getValidMetadataValue("double", 1.0)).isEqualTo(1.0);
    }

    /**
     * Verifies that a string metadata value is passed through unchanged.
     *
     * <p>When {@link MetadataUtil#getValidMetadataValue(String, Object)} is called with type {@code string} and
     * the value {@code "1"}, then it returns the string {@code "1"}.</p>
     */
    @Test
    public void shouldReturnStringValue() {
        assertThat(metadataUtil.getValidMetadataValue("string", "1")).isEqualTo("1");
    }

    /**
     * Verifies that a boolean metadata value is passed through unchanged.
     *
     * <p>When {@link MetadataUtil#getValidMetadataValue(String, Object)} is called with type {@code boolean} and
     * the value {@code true}, then it returns {@code true}.</p>
     */
    @Test
    public void shouldReturnBooleanValue() {
        assertThat(metadataUtil.getValidMetadataValue("boolean", true)).isEqualTo(true);
    }

    /**
     * Verifies that an epoch-millis timestamp is coerced to a {@link LocalDateTime} under the
     * {@code TIMESTAMP_NTZ} fixture.
     *
     * <p>Given the default {@code TIMESTAMP_NTZ} configuration and UTC zone, when
     * {@link MetadataUtil#getValidMetadataValue(String, Object)} is called with type {@code timestamp} and the
     * epoch-millis value for December 20, 2024 01:00:00, then it returns the corresponding
     * {@link LocalDateTime}.</p>
     */
    @Test
    public void shouldReturnTimestampNtzValue() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        long epoch = 1734656400000L; // December 20th, 2024 01:00:00
        LocalDateTime expectedValue = LocalDateTime.of(2024, 12, 20, 1, 0, 0);

        assertThat(metadataUtil.getValidMetadataValue("timestamp", epoch)).isEqualTo(expectedValue);
    }

    /**
     * Verifies that an epoch-millis timestamp is coerced to a {@link Timestamp} under the {@code TIMESTAMP}
     * fixture.
     *
     * <p>Given a {@link MetadataUtil} configured with {@link MaxComputeTimestampDataType#TIMESTAMP} and the UTC
     * zone, when {@link MetadataUtil#getValidMetadataValue(String, Object)} is called with type {@code timestamp}
     * and the epoch-millis value for December 20, 2024 01:00:00, then it returns the matching
     * {@link Timestamp}.</p>
     */
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
