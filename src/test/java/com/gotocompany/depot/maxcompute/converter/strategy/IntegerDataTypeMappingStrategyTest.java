package com.gotocompany.depot.maxcompute.converter.strategy;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class IntegerDataTypeMappingStrategyTest {

    private final IntegerDataTypeMappingStrategy integerPrimitiveProtobufMappingStrategy = new IntegerDataTypeMappingStrategy();

    @Test
    public void shouldMapProtoInt64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.INT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoInt32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.INT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    @Test
    public void shouldMapProtoUint64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.UINT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoUint32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.UINT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    @Test
    public void shouldMapProtoFixed64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.FIXED64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoFixed32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.FIXED32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    @Test
    public void shouldMapProtoSfixed64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SFIXED64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoSfixed32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SFIXED32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    @Test
    public void shouldMapProtoSint64ToOdpsBigInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SINT64);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.BIGINT);
    }

    @Test
    public void shouldMapProtoSint32ToOdpsInt() {
        TypeInfo typeInfo = integerPrimitiveProtobufMappingStrategy.getProtoTypeMap().get(Descriptors.FieldDescriptor.Type.SINT32);

        assertThat(typeInfo).isEqualTo(TypeInfoFactory.INT);
    }

    @Test
    public void shouldMapProtoInt64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.INT64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoInt32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.INT32).apply(1)).isEqualTo(1);
    }

    @Test
    public void shouldMapProtoUint64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.UINT64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoUint32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.UINT32).apply(1)).isEqualTo(1);
    }

    @Test
    public void shouldMapProtoFixed64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FIXED64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoFixed32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.FIXED32).apply(1)).isEqualTo(1);
    }

    @Test
    public void shouldMapProtoSfixed64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SFIXED64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoSfixed32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SFIXED32).apply(1)).isEqualTo(1);
    }

    @Test
    public void shouldMapProtoSint64ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SINT64).apply(1L)).isEqualTo(1L);
    }

    @Test
    public void shouldMapProtoSint32ToIdentityFunction() {
        assertThat(integerPrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.SINT32).apply(1)).isEqualTo(1);
    }
}
