package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StructProtobufMaxComputeConverterTest {

    private static final int STRUCT_INDEX = 4;

    private final StructProtobufMaxComputeConverter structProtobufMaxComputeConverter = new StructProtobufMaxComputeConverter();
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();

    @Test
    public void shouldConvertStructPayloadToJsonString() {
        Struct.Builder structBuilder = Struct.newBuilder();
        structBuilder.putFields("intField", Value.newBuilder().setNumberValue(1.0).build());
        structBuilder.putFields("stringField", Value.newBuilder().setStringValue("String").build());
        Message message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setStructField(structBuilder.build())
                .build();
        String expected = "{\"intField\":1.0,\"stringField\":\"String\"}";

        Object result = structProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)), 0));

        assertTrue(result instanceof String);
        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertRepeatedStructPayloadToJsonString() {
        Struct.Builder structBuilder = Struct.newBuilder();
        structBuilder.putFields("intField", Value.newBuilder().setNumberValue(1.0).build());
        structBuilder.putFields("stringField", Value.newBuilder().setStringValue("String").build());
        List<Struct> structs = new ArrayList<>();
        structs.add(structBuilder.build());
        structs.add(structBuilder.build());
        Message message = TestMaxComputeTypeInfo.TestRootRepeated.newBuilder()
                .addAllStructFields(structs)
                .build();
        String expected = "[{\"intField\":1.0,\"stringField\":\"String\"}, {\"intField\":1.0,\"stringField\":\"String\"}]";

        Object result = structProtobufMaxComputeConverter.convertPayload(new ProtoPayload(repeatedDescriptor.getFields().get(4), message.getField(repeatedDescriptor.getFields().get(4)), 0));

        assertTrue(result instanceof List);
        assertTrue(((List<?>) result).stream().allMatch(e -> e instanceof String));
        assertEquals(expected, result.toString());
    }

    @Test
    public void shouldConvertToStringTypeInfo() {
        TypeInfo typeInfo = structProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.getFields().get(STRUCT_INDEX)));

        assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

}
