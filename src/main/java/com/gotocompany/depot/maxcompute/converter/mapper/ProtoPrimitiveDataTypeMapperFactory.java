package com.gotocompany.depot.maxcompute.converter.mapper;

import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.DoubleToDecimalDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.FloatToDecimalDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.casted.IntegerToBigintDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.DoubleDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.FloatDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.IntegerDataTypeMapper;
import com.gotocompany.depot.maxcompute.converter.mapper.noncasted.NonNumericDataTypeMapper;

public class ProtoPrimitiveDataTypeMapperFactory {

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
