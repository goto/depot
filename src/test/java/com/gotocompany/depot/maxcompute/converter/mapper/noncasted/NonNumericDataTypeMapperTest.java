package com.gotocompany.depot.maxcompute.converter.mapper.noncasted;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NonNumericDataTypeMapper}, the primitive mapping strategy that handles the
 * non-numeric Protobuf scalar types: {@code BYTES}, {@code STRING}, {@code ENUM}, and {@code BOOL}.
 *
 * <p>The suite exercises the two lookup tables the strategy contributes:</p>
 * <ul>
 *     <li>{@link NonNumericDataTypeMapper#getProtoTypeMap()}, mapping each handled Protobuf type to its
 *     MaxCompute type (for example {@code BYTES} to {@code BINARY} and {@code BOOL} to {@code BOOLEAN});</li>
 *     <li>{@link NonNumericDataTypeMapper#getProtoPayloadMapperMap()}, mapping each handled Protobuf type to
 *     the function that converts a raw value into the object MaxCompute expects.</li>
 * </ul>
 *
 * <p>Each test resolves an entry by {@code Descriptors.FieldDescriptor.Type} against a fresh, real
 * {@link NonNumericDataTypeMapper} instance and asserts on the mapped type or converted value using AssertJ.</p>
 */
public class NonNumericDataTypeMapperTest {

    /**
     * The mapper under test, exercised through its public type and value lookup tables.
     */
    private final NonNumericDataTypeMapper basePrimitiveProtobufMappingStrategy = new NonNumericDataTypeMapper();

    /**
     * Verifies that the type map resolves the Protobuf {@code BYTES} type to the MaxCompute {@code BINARY}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.BYTES} in
     * {@link NonNumericDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BINARY}.</p>
     */
    @Test
    public void shouldMapProtoBytesToOdpsBinaryType() {
        Descriptors.FieldDescriptor.Type type = Descriptors.FieldDescriptor.Type.BYTES;

        assertThat(basePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(type))
                .isEqualTo(TypeInfoFactory.BINARY);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code STRING} type to the MaxCompute {@code STRING}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.STRING} in
     * {@link NonNumericDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.STRING}.</p>
     */
    @Test
    public void shouldMapProtoStringToOdpsStringType() {
        Descriptors.FieldDescriptor.Type type = Descriptors.FieldDescriptor.Type.STRING;

        assertThat(basePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(type))
                .isEqualTo(TypeInfoFactory.STRING);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code ENUM} type to the MaxCompute {@code STRING}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.ENUM} in
     * {@link NonNumericDataTypeMapper#getProtoTypeMap()} and asserts it equals {@code TypeInfoFactory.STRING},
     * confirming enums are stored as their string representation.</p>
     */
    @Test
    public void shouldMapProtoEnumToOdpsStringType() {
        Descriptors.FieldDescriptor.Type type = Descriptors.FieldDescriptor.Type.ENUM;

        assertThat(basePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(type))
                .isEqualTo(TypeInfoFactory.STRING);
    }

    /**
     * Verifies that the type map resolves the Protobuf {@code BOOL} type to the MaxCompute {@code BOOLEAN}
     * type.
     *
     * <p>Looks up {@code Descriptors.FieldDescriptor.Type.BOOL} in
     * {@link NonNumericDataTypeMapper#getProtoTypeMap()} and asserts it equals
     * {@code TypeInfoFactory.BOOLEAN}.</p>
     */
    @Test
    public void shouldMapProtoBoolToOdpsBooleanType() {
        Descriptors.FieldDescriptor.Type type = Descriptors.FieldDescriptor.Type.BOOL;

        assertThat(basePrimitiveProtobufMappingStrategy.getProtoTypeMap().get(type))
                .isEqualTo(TypeInfoFactory.BOOLEAN);
    }

    /**
     * Verifies that the {@code BYTES} value-conversion function wraps protobuf bytes in a MaxCompute
     * {@link Binary}.
     *
     * <p>Reads a {@code ByteString} from the bytes of the string {@code "test"}, applies the {@code BYTES}
     * mapper from {@link NonNumericDataTypeMapper#getProtoPayloadMapperMap()}, and asserts the result is a
     * {@link Binary} whose string form equals the original input.</p>
     *
     * @throws IOException if reading the input bytes into a {@code ByteString} fails
     */
    @Test
    public void shouldMapProtoBytesValue() throws IOException {
        Function<Object, Object> mapper = basePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.BYTES);
        String inputString = "test";
        ByteString input = ByteString.readFrom(new ByteArrayInputStream(inputString.getBytes()));

        Object result = mapper.apply(input);

        assertThat(result).isInstanceOf(Binary.class);
        assertThat(result.toString()).isEqualTo(inputString);
    }

    /**
     * Verifies that the {@code STRING} value-conversion function returns the input string unchanged.
     *
     * <p>Applies the {@code STRING} mapper from {@link NonNumericDataTypeMapper#getProtoPayloadMapperMap()} to
     * a sample string and asserts the result equals the input, confirming strings are forwarded as-is.</p>
     */
    @Test
    public void shouldMapProtoStringValue() {
        Function<Object, Object> mapper = basePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.STRING);
        String input = "test";

        Object result = mapper.apply(input);

        assertThat(result).isEqualTo(input);
    }

    /**
     * Verifies that the {@code ENUM} value-conversion function returns its input unchanged.
     *
     * <p>Applies the {@code ENUM} mapper from {@link NonNumericDataTypeMapper#getProtoPayloadMapperMap()} to a
     * sample value and asserts the result equals the input, confirming the enum value is forwarded as-is for
     * MaxCompute to store as a string.</p>
     */
    @Test
    public void shouldMapProtoEnumValue() {
        Function<Object, Object> mapper = basePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.ENUM);
        String input = "test";

        Object result = mapper.apply(input);

        assertThat(result).isEqualTo(input);
    }

    /**
     * Verifies that the {@code BOOL} value-conversion function returns its boolean input unchanged.
     *
     * <p>Applies the {@code BOOL} mapper from {@link NonNumericDataTypeMapper#getProtoPayloadMapperMap()} to
     * {@code true} and asserts the result equals the input.</p>
     */
    @Test
    public void shouldMapProtoBoolValue() {
        Function<Object, Object> mapper = basePrimitiveProtobufMappingStrategy.getProtoPayloadMapperMap().get(Descriptors.FieldDescriptor.Type.BOOL);
        boolean input = true;

        Object result = mapper.apply(input);

        assertThat(result).isEqualTo(input);
    }

}
