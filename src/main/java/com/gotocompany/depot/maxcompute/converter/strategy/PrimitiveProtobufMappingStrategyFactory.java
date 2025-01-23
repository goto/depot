package com.gotocompany.depot.maxcompute.converter.strategy;

import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class PrimitiveProtobufMappingStrategyFactory {

    public static PrimitiveProtobufMappingStrategy createPrimitiveProtobufMappingStrategy(MaxComputeSinkConfig maxComputeSinkConfig) {
        PrimitiveProtobufMappingStrategy basePrimitiveProtobufMappingStrategy = new BasePrimitiveProtobufMappingStrategy();
        PrimitiveProtobufMappingStrategy integerPrimitiveProtobufMappingStrategy = maxComputeSinkConfig.isProtoIntegerTypesToBigintEnabled()
                ? new UpcastedIntegerPrimitiveProtobufMappingStrategy() : new IntegerPrimitiveProtobufMappingStrategy();
        PrimitiveProtobufMappingStrategy floatPrimitiveProtobufMappingStrategy = maxComputeSinkConfig.isProtoFloatTypeToDecimalEnabled()
                ? new DecimalCastedFloatPrimitiveProtobufMappingStrategy(maxComputeSinkConfig) : new FloatPrimitiveProtobufMappingStrategy();
        PrimitiveProtobufMappingStrategy doublePrimitiveProtobufMappingStrategy = maxComputeSinkConfig.isProtoDoubleToDecimalEnabled()
                ? new DecimalCastedDoublePrimitiveProtobufMappingStrategy(maxComputeSinkConfig) : new DoublePrimitiveProtobufMappingStrategy();

        return basePrimitiveProtobufMappingStrategy.mergeStrategy(integerPrimitiveProtobufMappingStrategy)
                .mergeStrategy(floatPrimitiveProtobufMappingStrategy)
                .mergeStrategy(doublePrimitiveProtobufMappingStrategy);
    }

}
