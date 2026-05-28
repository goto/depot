package com.gotocompany.depot.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.CelTypes;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * Parses SINK_KAFKA_PROTO_MAPPING JSON at construction time, compiles each CEL expression
 * (fail-fast on syntax/type errors), and evaluates them per-message to produce a mapped
 * DynamicMessage for the sink proto.
 *
 * The source proto message is bound in the CEL environment under its fully-qualified class name,
 * matching the runtime variable name that callers must use in expressions.
 */
@Slf4j
public class ProtoMappingFunction {

    private final Map<String, CelRuntime.Program> compiledPrograms;
    private final Descriptors.Descriptor sinkDescriptor;
    private final String sourceProtoClass;

    public ProtoMappingFunction(
            String protoMappingJson,
            String sourceProtoClass,
            Descriptors.Descriptor sourceDescriptor,
            Descriptors.Descriptor sinkDescriptor) {
        this.sinkDescriptor = sinkDescriptor;
        this.sourceProtoClass = sourceProtoClass;

        Map<String, String> rawMapping = parseJson(protoMappingJson);
        this.compiledPrograms = compileAll(rawMapping, sourceProtoClass, sourceDescriptor);
    }

    /**
     * Evaluates all CEL expressions against the given source message and returns
     * a DynamicMessage built for the sink descriptor.
     *
     * @throws CelEvaluationException propagated per-message so callers can record ErrorInfo.
     */
    public DynamicMessage map(DynamicMessage sourceMessage) throws CelEvaluationException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(sinkDescriptor);
        for (Map.Entry<String, CelRuntime.Program> entry : compiledPrograms.entrySet()) {
            String fieldName = entry.getKey();
            CelRuntime.Program program = entry.getValue();

            Object result = program.eval(buildActivation(sourceMessage));

            Descriptors.FieldDescriptor fieldDescriptor = sinkDescriptor.findFieldByName(fieldName);
            if (fieldDescriptor == null) {
                log.warn("Sink proto field '{}' not found in descriptor '{}', skipping",
                        fieldName, sinkDescriptor.getFullName());
                continue;
            }
            builder.setField(fieldDescriptor, coerce(result, fieldDescriptor));
        }
        return builder.build();
    }

    private Map<String, Object> buildActivation(DynamicMessage sourceMessage) {
        Map<String, Object> activation = new HashMap<>();
        activation.put(sourceProtoClass, sourceMessage);
        return activation;
    }

    private Object coerce(Object value, Descriptors.FieldDescriptor fd) {
        if (value == null) {
            return fd.getDefaultValue();
        }
        // CEL returns Long for int expressions; proto may need int
        if (fd.getType() == Descriptors.FieldDescriptor.Type.INT32
                || fd.getType() == Descriptors.FieldDescriptor.Type.SINT32
                || fd.getType() == Descriptors.FieldDescriptor.Type.SFIXED32) {
            if (value instanceof Long) {
                return ((Long) value).intValue();
            }
        }
        return value;
    }

    private static Map<String, String> parseJson(String protoMappingJson) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(protoMappingJson, new TypeReference<Map<String, String>>() {
            });
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse SINK_KAFKA_PROTO_MAPPING JSON: " + e.getMessage(), e);
        }
    }

    private static Map<String, CelRuntime.Program> compileAll(
            Map<String, String> rawMapping,
            String sourceProtoClass,
            Descriptors.Descriptor sourceDescriptor) {
        CelCompiler compiler = CelCompilerFactory.standardCelCompilerBuilder()
                .addVar(sourceProtoClass, CelTypes.createMessage(sourceDescriptor.getFullName()))
                .addMessageTypes(sourceDescriptor)
                .build();

        CelRuntime runtime = CelRuntimeFactory.standardCelRuntimeBuilder()
                .build();

        Map<String, CelRuntime.Program> programs = new HashMap<>();
        for (Map.Entry<String, String> entry : rawMapping.entrySet()) {
            String fieldName = entry.getKey();
            String expression = entry.getValue();
            try {
                CelAbstractSyntaxTree ast = compiler.compile(expression).getAst();
                programs.put(fieldName, runtime.createProgram(ast));
            } catch (CelValidationException e) {
                throw new IllegalArgumentException(
                        "CEL compilation error for field '" + fieldName + "' expression '" + expression + "': " + e.getMessage(), e);
            } catch (CelEvaluationException e) {
                throw new IllegalArgumentException(
                        "CEL program creation error for field '" + fieldName + "': " + e.getMessage(), e);
            }
        }
        return programs;
    }
}
