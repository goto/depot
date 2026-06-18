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

/**
 * Unit tests for {@link StructProtobufMaxComputeConverter}, which maps the well-known Protobuf
 * {@code google.protobuf.Struct} type onto a MaxCompute {@code STRING}.
 *
 * <p>Because a {@code Struct} has a dynamic, schema-less shape, the converter serialises it to a JSON string
 * rather than to a nested MaxCompute struct. The suite builds {@code Struct} values from the
 * {@code TestMaxComputeTypeInfo} fixtures and asserts both the derived {@code STRING} type and the JSON
 * produced for singular and repeated struct fields. Each test uses a real converter instance with no
 * mocking.</p>
 */
public class StructProtobufMaxComputeConverterTest {

    /**
     * Index of the singular {@code struct_field} within the {@code TestRoot} descriptor.
     */
    private static final int STRUCT_INDEX = 4;

    /**
     * The converter under test.
     */
    private final StructProtobufMaxComputeConverter structProtobufMaxComputeConverter = new StructProtobufMaxComputeConverter();

    /**
     * Descriptor of the {@code TestRoot} fixture message, used to resolve the singular struct field.
     */
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();

    /**
     * Descriptor of the {@code TestRootRepeated} fixture message, used to resolve the repeated struct field.
     */
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();

    /**
     * Verifies that a singular {@code Struct} value is serialised to a JSON string.
     *
     * <p>Builds a {@code Struct} with one numeric and one string entry, converts the field via
     * {@link StructProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is the
     * JSON string {@code {"intField":1.0,"stringField":"String"}}.</p>
     */
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

    /**
     * Verifies that a repeated {@code Struct} field is serialised to a list of JSON strings.
     *
     * <p>Builds a {@code TestRootRepeated} message with two identical structs, converts the repeated field via
     * {@link StructProtobufMaxComputeConverter#convertPayload(ProtoPayload)}, and asserts the result is a
     * {@link java.util.List} whose elements are all strings and whose string form matches the expected JSON
     * array of the two serialised structs.</p>
     */
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

    /**
     * Verifies that the converter derives a {@code STRING} type for a struct field.
     *
     * <p>Resolves the struct field descriptor and asserts that
     * {@link StructProtobufMaxComputeConverter#convertTypeInfo(ProtoPayload)} returns
     * {@code TypeInfoFactory.STRING}, reflecting that structs are stored as JSON text.</p>
     */
    @Test
    public void shouldConvertToStringTypeInfo() {
        TypeInfo typeInfo = structProtobufMaxComputeConverter.convertTypeInfo(new ProtoPayload(descriptor.getFields().get(STRUCT_INDEX)));

        assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

}
