package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class BaseDataTypeMappingStrategyTest {

    private final NonNumericDataTypeMapper basePrimitiveProtobufMappingStrategy = new NonNumericDataTypeMapper();

    @Test
    public void shouldMapProtoBytesToOdpsBinaryType() {
        Descriptors.FieldDescriptor.Type type = Descriptors.FieldDescriptor.Type.BYTES;

        assertThat(basePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(type))
                .isEqualTo(TypeInfoFactory.BINARY);
    }

    @Test
    public void shouldMapProtoStringToOdpsStringType() {
        Descriptors.FieldDescriptor.Type type = Descriptors.FieldDescriptor.Type.STRING;

        assertThat(basePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(type))
                .isEqualTo(TypeInfoFactory.STRING);
    }

    @Test
    public void shouldMapProtoEnumToOdpsStringType() {
        Descriptors.FieldDescriptor.Type type = Descriptors.FieldDescriptor.Type.ENUM;

        assertThat(basePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(type))
                .isEqualTo(TypeInfoFactory.STRING);
    }

    @Test
    public void shouldMapProtoBoolToOdpsBooleanType() {
        Descriptors.FieldDescriptor.Type type = Descriptors.FieldDescriptor.Type.BOOL;

        assertThat(basePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(type))
                .isEqualTo(TypeInfoFactory.BOOLEAN);
    }

    @Test
    public void shouldMapProtoBytesValue() throws IOException {
        Function<Object, Object> mapper = basePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.BYTES);
        String inputString = "test";
        ByteString input = ByteString.readFrom(new ByteArrayInputStream(inputString.getBytes()));

        Object result = mapper.apply(input);

        assertThat(result).isInstanceOf(Binary.class);
        assertThat(result.toString()).isEqualTo(inputString);
    }

    @Test
    public void shouldMapProtoStringValue() {
        Function<Object, Object> mapper = basePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.STRING);
        String input = "test";

        Object result = mapper.apply(input);

        assertThat(result).isEqualTo(input);
    }

    @Test
    public void shouldMapProtoEnumValue() {
        Function<Object, Object> mapper = basePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.ENUM);
        String input = "test";

        Object result = mapper.apply(input);

        assertThat(result).isEqualTo(input);
    }

    @Test
    public void shouldMapProtoBoolValue() {
        Function<Object, Object> mapper = basePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.BOOL);
        boolean input = true;

        Object result = mapper.apply(input);

        assertThat(result).isEqualTo(input);
    }

}
