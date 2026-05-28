package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import dev.cel.parser.CelStandardMacro;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Compiles and evaluates {@code SINK_KAFKA_PROTO_MAPPING} CEL expressions against the source proto.
 */
public class ProtoMappingEngine {

    static final String SOURCE_VAR = "source";

    private final List<FieldMapping> fieldMappings;
    private final Descriptors.Descriptor sinkKeyDescriptor;
    private final Descriptors.Descriptor sinkMessageDescriptor;

    private ProtoMappingEngine(
            List<FieldMapping> fieldMappings,
            Descriptors.Descriptor sinkKeyDescriptor,
            Descriptors.Descriptor sinkMessageDescriptor) {
        this.fieldMappings = fieldMappings;
        this.sinkKeyDescriptor = sinkKeyDescriptor;
        this.sinkMessageDescriptor = sinkMessageDescriptor;
    }

    public static ProtoMappingEngine create(
            String protoMappingJson,
            Descriptors.Descriptor sourceDescriptor,
            Descriptors.Descriptor sinkKeyDescriptor,
            Descriptors.Descriptor sinkMessageDescriptor) throws CelValidationException, CelEvaluationException {
        JSONObject mapping = new JSONObject(protoMappingJson);
        CelCompiler compiler = CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.HAS)
                .addMessageTypes(sourceDescriptor, sinkKeyDescriptor, sinkMessageDescriptor)
                .addVar(SOURCE_VAR, StructTypeReference.create(sourceDescriptor.getFullName()))
                .build();
        CelRuntime runtime = CelRuntimeFactory.standardCelRuntimeBuilder()
                .addMessageTypes(sourceDescriptor, sinkKeyDescriptor, sinkMessageDescriptor)
                .build();

        List<FieldMapping> mappings = new ArrayList<>();
        Iterator<String> keys = mapping.keys();
        while (keys.hasNext()) {
            String fieldName = keys.next();
            boolean onKey = sinkKeyDescriptor.findFieldByName(fieldName) != null;
            boolean onMessage = sinkMessageDescriptor.findFieldByName(fieldName) != null;
            if (onKey == onMessage) {
                throw new IllegalArgumentException("Ambiguous or unknown mapping field: " + fieldName);
            }
            Descriptors.Descriptor targetDesc = onKey ? sinkKeyDescriptor : sinkMessageDescriptor;
            Descriptors.FieldDescriptor fd = targetDesc.findFieldByName(fieldName);
            CelRuntime.Program program = runtime.createProgram(
                    compiler.compile(mapping.getString(fieldName)).getAst());
            mappings.add(new FieldMapping(fd, onKey, program));
        }
        return new ProtoMappingEngine(mappings, sinkKeyDescriptor, sinkMessageDescriptor);
    }

    public MappedRecord map(Message source) throws CelEvaluationException {
        DynamicMessage.Builder keyBuilder = DynamicMessage.newBuilder(sinkKeyDescriptor);
        DynamicMessage.Builder msgBuilder = DynamicMessage.newBuilder(sinkMessageDescriptor);
        Map<String, Object> activation = Collections.singletonMap(SOURCE_VAR, source);

        for (FieldMapping m : fieldMappings) {
            Object value = toProtoValue(m.program.eval(activation), m.field);
            DynamicMessage.Builder builder = m.onKey ? keyBuilder : msgBuilder;
            if (m.field.isRepeated()) {
                builder.setField(m.field, (List<?>) value);
            } else {
                builder.setField(m.field, value);
            }
        }
        return new MappedRecord(keyBuilder.build(), msgBuilder.build());
    }

    private static Object toProtoValue(Object celValue, Descriptors.FieldDescriptor field) {
        if (celValue == null) {
            return field.isRepeated() ? Collections.emptyList() : field.getDefaultValue();
        }
        if (field.isRepeated()) {
            List<?> raw = celValue instanceof List
                    ? (List<?>) celValue
                    : Collections.singletonList(celValue);
            List<Object> out = new ArrayList<>(raw.size());
            for (Object item : raw) {
                out.add(toSingularValue(item, field));
            }
            return out;
        }
        return toSingularValue(celValue, field);
    }

    private static Object toSingularValue(Object celValue, Descriptors.FieldDescriptor field) {
        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return celValue;
        }
        switch (field.getJavaType()) {
            case INT:
                return celValue instanceof Number
                        ? ((Number) celValue).intValue()
                        : Integer.parseInt(String.valueOf(celValue));
            case LONG:
                return celValue instanceof Number
                        ? ((Number) celValue).longValue()
                        : Long.parseLong(String.valueOf(celValue));
            case FLOAT:
                return celValue instanceof Number
                        ? ((Number) celValue).floatValue()
                        : Float.parseFloat(String.valueOf(celValue));
            case DOUBLE:
                return celValue instanceof Number
                        ? ((Number) celValue).doubleValue()
                        : Double.parseDouble(String.valueOf(celValue));
            case BOOLEAN:
                return celValue instanceof Boolean ? celValue : Boolean.parseBoolean(String.valueOf(celValue));
            case STRING:
                return String.valueOf(celValue);
            case ENUM:
                if (celValue instanceof Descriptors.EnumValueDescriptor) {
                    return celValue;
                }
                if (celValue instanceof Number) {
                    return field.getEnumType().findValueByNumber(((Number) celValue).intValue());
                }
                return field.getEnumType().findValueByName(String.valueOf(celValue));
            default:
                return celValue;
        }
    }

    private static final class FieldMapping {
        final Descriptors.FieldDescriptor field;
        final boolean onKey;
        final CelRuntime.Program program;

        FieldMapping(Descriptors.FieldDescriptor field, boolean onKey, CelRuntime.Program program) {
            this.field = field;
            this.onKey = onKey;
            this.program = program;
        }
    }

    public static final class MappedRecord {
        private final DynamicMessage key;
        private final DynamicMessage message;

        MappedRecord(DynamicMessage key, DynamicMessage message) {
            this.key = key;
            this.message = message;
        }

        public DynamicMessage getKey() {
            return key;
        }

        public DynamicMessage getMessage() {
            return message;
        }
    }
}
