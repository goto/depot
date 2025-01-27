package com.gotocompany.depot.maxcompute.converter.strategy;

import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class ProtoPrimitiveDataTypeMappingStrategyFactory {

    public static ProtoPrimitiveDataTypeMapper createPrimitiveProtobufMappingStrategy(MaxComputeSinkConfig maxComputeSinkConfig) {
        ProtoPrimitiveDataTypeMapper baseProtoPrimitiveDataTypeMapper = new NonNumericDataTypeMapper();
        ProtoPrimitiveDataTypeMapper integerProtoPrimitiveDataTypeMapper = maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()
                ? new IntegerToBigintDataTypeMapper() : new IntegerDataTypeMapper();
        ProtoPrimitiveDataTypeMapper floatProtoPrimitiveDataTypeMapper = maxComputeSinkConfig.isProtoFloatTypeToDecimalEnabled()
                ? new FloatToDecimalDataTypeMapper(maxComputeSinkConfig) : new FloatDataTypeMapper();
        ProtoPrimitiveDataTypeMapper doubleProtoPrimitiveDataTypeMapper = maxComputeSinkConfig.isProtoDoubleToDecimalEnabled()
                ? new DoubleToDecimalDataTypeMapper(maxComputeSinkConfig) : new DoubleDataTypeMapper();

        return baseProtoPrimitiveDataTypeMapper.mergeStrategy(integerProtoPrimitiveDataTypeMapper)
                .mergeStrategy(floatProtoPrimitiveDataTypeMapper)
                .mergeStrategy(doubleProtoPrimitiveDataTypeMapper);
    }

}
