package com.gotocompany.depot.maxcompute.converter.strategy;

import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class ProtoPrimitiveDataTypeMappingStrategyFactory {

    public static ProtoPrimitiveDataTypeMappingStrategy createPrimitiveProtobufMappingStrategy(MaxComputeSinkConfig maxComputeSinkConfig) {
        ProtoPrimitiveDataTypeMappingStrategy baseProtoPrimitiveDataTypeMappingStrategy = new BaseDataTypeMappingStrategy();
        ProtoPrimitiveDataTypeMappingStrategy integerProtoPrimitiveDataTypeMappingStrategy = maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()
                ? new IntegerToBigintDataTypeMappingStrategy() : new IntegerDataTypeMappingStrategy();
        ProtoPrimitiveDataTypeMappingStrategy floatProtoPrimitiveDataTypeMappingStrategy = maxComputeSinkConfig.isProtoFloatTypeToDecimalEnabled()
                ? new FloatToDecimalDataTypeMappingStrategy(maxComputeSinkConfig) : new FloatDataTypeMappingStrategy();
        ProtoPrimitiveDataTypeMappingStrategy doubleProtoPrimitiveDataTypeMappingStrategy = maxComputeSinkConfig.isProtoDoubleToDecimalEnabled()
                ? new DoubleToDecimalDataTypeMappingStrategy(maxComputeSinkConfig) : new DoubleDataTypeMappingStrategy();

        return baseProtoPrimitiveDataTypeMappingStrategy.mergeStrategy(integerProtoPrimitiveDataTypeMappingStrategy)
                .mergeStrategy(floatProtoPrimitiveDataTypeMappingStrategy)
                .mergeStrategy(doubleProtoPrimitiveDataTypeMappingStrategy);
    }

}
