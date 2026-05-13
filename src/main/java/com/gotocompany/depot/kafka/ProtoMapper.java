package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ProtoMapper {

    private final Map<String, CelRuntime.Program> compiledPrograms;
    private final Descriptors.Descriptor sinkDescriptor;
    private final String sourceVariableName;

    public ProtoMapper(
            Descriptors.Descriptor sourceDescriptor,
            Descriptors.Descriptor sinkDescriptor,
            String mappingJson) {
        this.sinkDescriptor = sinkDescriptor;
        this.sourceVariableName = sourceDescriptor.getFullName();

        CelCompiler compiler = CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                .addMessageTypes(sourceDescriptor)
                .addMessageTypes(sinkDescriptor)
                .addVar(sourceVariableName, StructTypeReference.create(sourceDescriptor.getFullName()))
                .build();

        CelRuntime runtime = CelRuntimeFactory.standardCelRuntimeBuilder()
                .addMessageTypes(sourceDescriptor)
                .addMessageTypes(sinkDescriptor)
                .build();

        JSONObject mapping = new JSONObject(mappingJson);
        this.compiledPrograms = new HashMap<>();

        for (String outputField : mapping.keySet()) {
            String celExpression = mapping.getString(outputField);
            try {
                CelAbstractSyntaxTree ast = compiler.compile(celExpression).getAst();
                CelRuntime.Program program = runtime.createProgram(ast);
                compiledPrograms.put(outputField, program);
            } catch (CelValidationException | CelEvaluationException e) {
                throw new IllegalArgumentException(
                        "Failed to compile CEL expression for field '" + outputField
                                + "': " + celExpression + ". Error: " + e.getMessage(), e);
            }
        }
        log.info("Compiled {} CEL mapping expressions successfully", compiledPrograms.size());
    }

    public DynamicMessage map(DynamicMessage sourceMessage) throws CelEvaluationException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(sinkDescriptor);
        Map<String, Object> bindings = new HashMap<>();
        bindings.put(sourceVariableName, sourceMessage);

        for (Map.Entry<String, CelRuntime.Program> entry : compiledPrograms.entrySet()) {
            String outputFieldName = entry.getKey();
            CelRuntime.Program program = entry.getValue();

            Descriptors.FieldDescriptor fieldDescriptor = sinkDescriptor.findFieldByName(outputFieldName);
            if (fieldDescriptor == null) {
                throw new CelEvaluationException(
                        "Output field '" + outputFieldName + "' not found in sink proto "
                                + sinkDescriptor.getFullName());
            }

            Object result = program.eval(bindings);
            if (result != null) {
                builder.setField(fieldDescriptor, coerceType(result, fieldDescriptor));
            }
        }
        return builder.build();
    }

    private Object coerceType(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getJavaType()) {
            case INT:
                if (value instanceof Long) {
                    return ((Long) value).intValue();
                }
                if (value instanceof Double) {
                    return ((Double) value).intValue();
                }
                return ((Number) value).intValue();
            case LONG:
                if (value instanceof Integer) {
                    return ((Integer) value).longValue();
                }
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BOOLEAN:
                return value;
            case STRING:
                return value.toString();
            case ENUM:
                if (value instanceof Integer) {
                    return fieldDescriptor.getEnumType().findValueByNumber((Integer) value);
                }
                if (value instanceof Long) {
                    return fieldDescriptor.getEnumType().findValueByNumber(((Long) value).intValue());
                }
                if (value instanceof String) {
                    return fieldDescriptor.getEnumType().findValueByName((String) value);
                }
                return value;
            default:
                return value;
        }
    }
}
