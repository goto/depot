package com.gotocompany.depot.maxcompute.enumeration;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import lombok.Getter;

/**
 * Enumerates the MaxCompute physical timestamp types that a Protobuf timestamp value can be mapped
 * to when it is written to a MaxCompute table.
 *
 * <p>Depot's MaxCompute sink lets operators choose how Protobuf timestamp values (for example
 * {@code google.protobuf.Timestamp} fields and timestamp-typed metadata) are materialised in the
 * destination table. Each constant binds a logical choice to the concrete {@link TypeInfo}
 * understood by the MaxCompute SDK:</p>
 * <ul>
 *     <li>{@link #TIMESTAMP} maps to a timezone-aware instant.</li>
 *     <li>{@link #TIMESTAMP_NTZ} maps to a timezone-naive (local) date-time.</li>
 * </ul>
 *
 * <p>The selected constant is resolved from the sink configuration and is consulted by helpers such
 * as {@link com.gotocompany.depot.maxcompute.util.MetadataUtil} when building column schemas and
 * converting metadata values. The mapped {@link TypeInfo} is exposed through the Lombok-generated
 * {@code getTypeInfo()} accessor.</p>
 */
@Getter
public enum MaxComputeTimestampDataType {

    /**
     * Represents the MaxCompute {@code TIMESTAMP} data type, which models an instant with timezone
     * semantics (a point on the UTC time-line). Backed by {@link TypeInfoFactory#TIMESTAMP}.
     */
    TIMESTAMP(TypeInfoFactory.TIMESTAMP),
    /**
     * Represents the MaxCompute {@code TIMESTAMP_NTZ} (no time zone) data type, which models a local
     * date-time carrying no timezone offset. Backed by {@link TypeInfoFactory#TIMESTAMP_NTZ}.
     */
    TIMESTAMP_NTZ(TypeInfoFactory.TIMESTAMP_NTZ);

    /**
     * The MaxCompute {@link TypeInfo} that this constant maps to; supplied at construction time and
     * exposed through the Lombok-generated {@code getTypeInfo()} accessor.
     */
    private final TypeInfo typeInfo;

    /**
     * Creates a timestamp data-type mapping bound to the given MaxCompute {@link TypeInfo}.
     *
     * @param typeInfo the MaxCompute type information associated with this constant
     */
    MaxComputeTimestampDataType(TypeInfo typeInfo) {
        this.typeInfo = typeInfo;
    }

}
