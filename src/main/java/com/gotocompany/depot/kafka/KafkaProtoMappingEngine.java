package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.stencil.client.StencilClient;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.HashMap;
import java.util.Map;

/**
 * Compiles and evaluates CEL expressions defined in SINK_KAFKA_PROTO_MAPPING
 * to map fields from source proto messages to sink proto messages.
 */
public class KafkaProtoMappingEngine {

    private static final String SOURCE_VARIABLE = "source";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Map<String, CelRuntime.Program> compiledPrograms;
    private final Descriptors.Descriptor sinkMessageDescriptor;
    private final Descriptors.Descriptor sinkKeyDescriptor;
    private final CelRuntime celRuntime;

    public KafkaProtoMappingEngine(
            KafkaSinkConfig config,
            StencilClient sourceStencilClient,
            StencilClient sinkStencilClient,
            Instrumentation instrumentation) {

        String sourceMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
        String sinkMessageClass = config.getSinkKafkaProtoMessage();
        String sinkKeyClass = config.getSinkKafkaProtoKey();
        String mappingJson = config.getSinkKafkaProtoMapping();

        Descriptors.Descriptor sourceDescriptor = sourceStencilClient.get(sourceMessageClass);
        if (sourceDescriptor == null) {
            throw new IllegalArgumentException("Source proto descriptor not found for: " + sourceMessageClass);
        }

        this.sinkMessageDescriptor = sinkStencilClient.get(sinkMessageClass);
        if (sinkMessageDescriptor == null) {
            throw new IllegalArgumentException("Sink message proto descriptor not found for: " + sinkMessageClass);
        }

        if (sinkKeyClass != null && !sinkKeyClass.isEmpty()) {
            this.sinkKeyDescriptor = sinkStencilClient.get(sinkKeyClass);
            if (sinkKeyDescriptor == null) {
                throw new IllegalArgumentException("Sink key proto descriptor not found for: " + sinkKeyClass);
            }
        } else {
            this.sinkKeyDescriptor = null;
        }

        // Parse mapping JSON
        Map<String, String> fieldMappings = parseMappingJson(mappingJson);

        // Build CEL compiler with source proto type
        CelCompiler celCompiler = CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.HAS, CelStandardMacro.ALL,
                        CelStandardMacro.EXISTS, CelStandardMacro.EXISTS_ONE,
                        CelStandardMacro.MAP, CelStandardMacro.FILTER)
                .addMessageTypes(sourceDescriptor)
                .addVar(SOURCE_VARIABLE, StructTypeReference.create(sourceDescriptor.getFullName()))
                .build();

        this.celRuntime = CelRuntimeFactory.standardCelRuntimeBuilder()
                .addMessageTypes(sourceDescriptor)
                .build();

        // Compile all CEL expressions at startup (fail-fast)
        this.compiledPrograms = new HashMap<>();
        for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
            String fieldName = entry.getKey();
            String celExpression = entry.getValue();
            try {
                CelAbstractSyntaxTree ast = celCompiler.compile(celExpression).getAst();
                CelRuntime.Program program = celRuntime.createProgram(ast);
                compiledPrograms.put(fieldName, program);
                instrumentation.logInfo("Compiled CEL expression for field '{}': {}", fieldName, celExpression);
            } catch (CelValidationException e) {
                throw new IllegalArgumentException(
                        "Failed to compile CEL expression for field '" + fieldName + "': " + celExpression, e);
            } catch (CelEvaluationException e) {
                throw new IllegalArgumentException(
                        "Failed to create program for field '" + fieldName + "': " + celExpression, e);
            }
        }

        instrumentation.logInfo("KafkaProtoMappingEngine initialized with {} field mappings", compiledPrograms.size());
    }

    /**
     * Maps a source DynamicMessage to a sink message DynamicMessage using compiled CEL expressions.
     */
    public DynamicMessage mapToSinkMessage(DynamicMessage sourceMessage) throws CelEvaluationException {
        return mapToProto(sourceMessage, sinkMessageDescriptor);
    }

    /**
     * Maps a source DynamicMessage to a sink key DynamicMessage using compiled CEL expressions.
     * Only fields present in the key descriptor will be mapped.
     */
    public DynamicMessage mapToSinkKey(DynamicMessage sourceMessage) throws CelEvaluationException {
        if (sinkKeyDescriptor == null) {
            return null;
        }
        return mapToProto(sourceMessage, sinkKeyDescriptor);
    }

    /**
     * Evaluates a single CEL expression against a source message. Useful for testing.
     */
    public Object evaluateField(String fieldName, DynamicMessage sourceMessage) throws CelEvaluationException {
        CelRuntime.Program program = compiledPrograms.get(fieldName);
        if (program == null) {
            throw new CelEvaluationException("No compiled program found for field: " + fieldName);
        }
        Map<String, Object> bindings = new HashMap<>();
        bindings.put(SOURCE_VARIABLE, sourceMessage);
        return program.eval(bindings);
    }

    public Descriptors.Descriptor getSinkMessageDescriptor() {
        return sinkMessageDescriptor;
    }

    public Descriptors.Descriptor getSinkKeyDescriptor() {
        return sinkKeyDescriptor;
    }

    private DynamicMessage mapToProto(DynamicMessage sourceMessage, Descriptors.Descriptor targetDescriptor)
            throws CelEvaluationException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(targetDescriptor);
        Map<String, Object> bindings = new HashMap<>();
        bindings.put(SOURCE_VARIABLE, sourceMessage);

        for (Map.Entry<String, CelRuntime.Program> entry : compiledPrograms.entrySet()) {
            String fieldName = entry.getKey();
            CelRuntime.Program program = entry.getValue();

            Descriptors.FieldDescriptor fieldDescriptor = targetDescriptor.findFieldByName(fieldName);
            if (fieldDescriptor == null) {
                // Field not present in this target proto — skip (supports D1: single mapping for both key and message)
                continue;
            }

            Object result = program.eval(bindings);
            if (result != null) {
                builder.setField(fieldDescriptor, coerceValue(result, fieldDescriptor));
            }
        }
        return builder.build();
    }

    /**
     * Coerce a CEL evaluation result to the expected protobuf field type.
     */
    private Object coerceValue(Object value, Descriptors.FieldDescriptor fieldDescriptor) {
        if (value == null) {
            return fieldDescriptor.getDefaultValue();
        }

        switch (fieldDescriptor.getType()) {
            case STRING:
                return value.toString();
            case INT32:
            case SINT32:
            case SFIXED32:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return Integer.parseInt(value.toString());
            case INT64:
            case SINT64:
            case SFIXED64:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return Long.parseLong(value.toString());
            case UINT32:
            case FIXED32:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return Integer.parseUnsignedInt(value.toString());
            case UINT64:
            case FIXED64:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return Long.parseUnsignedLong(value.toString());
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                return Float.parseFloat(value.toString());
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return Double.parseDouble(value.toString());
            case BOOL:
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.parseBoolean(value.toString());
            case ENUM:
                if (value instanceof Number) {
                    return fieldDescriptor.getEnumType().findValueByNumber(((Number) value).intValue());
                }
                if (value instanceof String) {
                    return fieldDescriptor.getEnumType().findValueByName((String) value);
                }
                return value;
            case MESSAGE:
                if (value instanceof DynamicMessage) {
                    return value;
                }
                if (value instanceof Message) {
                    try {
                        return DynamicMessage.parseFrom(
                                fieldDescriptor.getMessageType(),
                                ((Message) value).toByteArray());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("Failed to convert message type for field: " + fieldDescriptor.getName(), e);
                    }
                }
                return value;
            case BYTES:
                return value;
            default:
                return value;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseMappingJson(String mappingJson) {
        if (mappingJson == null || mappingJson.isEmpty()) {
            throw new IllegalArgumentException("SINK_KAFKA_PROTO_MAPPING cannot be null or empty");
        }
        try {
            return OBJECT_MAPPER.readValue(mappingJson, Map.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse SINK_KAFKA_PROTO_MAPPING as JSON: " + mappingJson, e);
        }
    }
}
