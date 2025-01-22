package com.gotocompany.depot.maxcompute.converter.initializer;

import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class PrimitiveProtobufMappingStrategyFactory {

    public static PrimitiveProtobufMappingStrategy createPrimitiveProtobufMappingStrategy(MaxComputeSinkConfig maxComputeSinkConfig) {
        PrimitiveProtobufMappingStrategy basePrimitiveProtobufMappingStrategy = new BasePrimitiveProtobufMappingStrategy();
        PrimitiveProtobufMappingStrategy integerPrimitiveProtobufMappingStrategy = maxComputeSinkConfig.isUpcastIntegerTypesEnabled() ?
                new UpcastedIntegerPrimitiveProtobufMappingStrategy() : new IntegerPrimitiveProtobufMappingStrategy();
        PrimitiveProtobufMappingStrategy floatPrimitiveProtobufMappingStrategy = maxComputeSinkConfig.isCastFloatToDecimalEnabled() ?
                new DecimalCastedFloatPrimitiveProtobufMappingStrategy(maxComputeSinkConfig) : new FloatPrimitiveProtobufMappingStrategy();
        PrimitiveProtobufMappingStrategy doublePrimitiveProtobufMappingStrategy = maxComputeSinkConfig.isCastDoubleToDecimalEnabled() ?
                new DecimalCastedDoublePrimitiveProtobufMappingStrategy(maxComputeSinkConfig) : new DoublePrimitiveProtobufMappingStrategy();

        return basePrimitiveProtobufMappingStrategy.mergeStrategy(integerPrimitiveProtobufMappingStrategy)
                .mergeStrategy(floatPrimitiveProtobufMappingStrategy)
                .mergeStrategy(doublePrimitiveProtobufMappingStrategy);
    }

}
