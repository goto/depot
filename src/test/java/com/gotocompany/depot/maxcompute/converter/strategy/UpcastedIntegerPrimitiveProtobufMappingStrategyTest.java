package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UpcastedIntegerPrimitiveProtobufMappingStrategyTest {

    private final UpcastedIntegerPrimitiveProtobufMappingStrategy upcastedIntegerPrimitiveProtobufMappingStrategy = new UpcastedIntegerPrimitiveProtobufMappingStrategy();

    @Test
    public void shouldMapProtoInt64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.INT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoInt32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.INT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoUint64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.UINT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoUint32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.UINT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoFixed64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.FIXED64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoFixed32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.FIXED32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoSfixed64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SFIXED64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoSfixed32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SFIXED32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoSint64ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SINT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoSint32ToOdpsBigInt() {
        TypeInfo typeInfo = upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SINT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoInt64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.INT64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoInt32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.INT32).apply(1)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoUint64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.UINT64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoUint32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.UINT32).apply(1)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoFixed64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FIXED64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoFixed32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FIXED32).apply(1)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoSfixed64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SFIXED64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoSfixed32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SFIXED32).apply(1)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoSint64Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SINT64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoSint32Value() {
        AssertionsForClassTypes.assertThat(upcastedIntegerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SINT32).apply(1)).isEqualTo(1L);
    }

}
