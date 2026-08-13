package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.ConfigurationException;
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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Compiles the configured proto mapping into a {@link ProtoMappingFunction} for the sink schemas.
 *
 * <p>Each CEL expression in {@code SINK_KAFKA_PROTO_MAPPING} is compiled against the source proto
 * descriptor with the target field type as its declared result type, so type mismatches and unknown
 * fields fail fast at startup rather than at runtime.
 */
@Slf4j
public class ProtoMappingFunctionFactory {

    /**
     * Compiles the proto mapping into a mapping function for the value and optional key proto schemas.
     *
     * @param sourceDescriptor the descriptor of the source proto message
     * @param valueDescriptor  the descriptor of the sink value proto message
     * @param keyDescriptor    the descriptor of the sink key proto message, or {@code null} when no key proto is configured
     * @param protoMapping     the mapping of output field names to CEL expressions
     * @return the compiled mapping function
     * @throws ConfigurationException if the mapping is empty, references unknown fields or contains invalid expressions
     */
    public ProtoMappingFunction create(Descriptors.Descriptor sourceDescriptor,
                                       Descriptors.Descriptor valueDescriptor,
                                       Descriptors.Descriptor keyDescriptor,
                                       Map<String, String> protoMapping) {
        if (protoMapping == null || protoMapping.isEmpty()) {
            throw new ConfigurationException("SINK_KAFKA_PROTO_MAPPING should contain at least one field mapping");
        }
        validateMappedFields(valueDescriptor, keyDescriptor, protoMapping);
        Set<Descriptors.FileDescriptor> fileDescriptors = collectFileDescriptors(sourceDescriptor, valueDescriptor, keyDescriptor);
        CelRuntime celRuntime = CelRuntimeFactory.standardCelRuntimeBuilder()
                .addFileTypes(fileDescriptors)
                .build();
        CelValueConverter celValueConverter = new CelValueConverter();
        Map<Descriptors.FieldDescriptor, CelRuntime.Program> valuePrograms =
                compileFieldPrograms(sourceDescriptor, valueDescriptor, protoMapping, fileDescriptors, celRuntime);
        if (valuePrograms.isEmpty()) {
            log.warn("none of the fields defined in SINK_KAFKA_PROTO_MAPPING are present in the value proto {},"
                    + " all the output message values will be serialized default instances", valueDescriptor.getFullName());
        }
        ProtoMessageMapper valueMapper = new ProtoMessageMapper(valueDescriptor, valuePrograms, celValueConverter);
        ProtoMessageMapper keyMapper = null;
        if (keyDescriptor != null) {
            Map<Descriptors.FieldDescriptor, CelRuntime.Program> keyPrograms =
                    compileFieldPrograms(sourceDescriptor, keyDescriptor, protoMapping, fileDescriptors, celRuntime);
            if (keyPrograms.isEmpty()) {
                log.warn("none of the fields defined in SINK_KAFKA_PROTO_MAPPING are present in the key proto {},"
                        + " all the output record keys will be serialized default instances", keyDescriptor.getFullName());
            }
            keyMapper = new ProtoMessageMapper(keyDescriptor, keyPrograms, celValueConverter);
        }
        return new ProtoMappingFunction(keyMapper, valueMapper);
    }

    /**
     * Validates that every mapped field exists in the value proto or the key proto.
     *
     * @param valueDescriptor the descriptor of the sink value proto message
     * @param keyDescriptor   the descriptor of the sink key proto message, or {@code null} when no key proto is configured
     * @param protoMapping    the mapping of output field names to CEL expressions
     * @throws ConfigurationException if any mapped field is absent from both output protos
     */
    private void validateMappedFields(Descriptors.Descriptor valueDescriptor,
                                      Descriptors.Descriptor keyDescriptor,
                                      Map<String, String> protoMapping) {
        List<String> unknownFields = protoMapping.keySet().stream()
                .filter(fieldName -> valueDescriptor.findFieldByName(fieldName) == null
                        && (keyDescriptor == null || keyDescriptor.findFieldByName(fieldName) == null))
                .collect(Collectors.toList());
        if (!unknownFields.isEmpty()) {
            throw new ConfigurationException(
                    String.format("fields %s defined in SINK_KAFKA_PROTO_MAPPING are not present in the output proto schemas", unknownFields));
        }
    }

    /**
     * Compiles the CEL programs for every mapped field that exists in the target proto.
     *
     * @param sourceDescriptor the descriptor of the source proto message
     * @param targetDescriptor the descriptor of the proto message being populated
     * @param protoMapping     the mapping of output field names to CEL expressions
     * @param fileDescriptors  the proto file descriptors registered with the CEL environment
     * @param celRuntime       the CEL runtime used to create the programs
     * @return the compiled programs keyed by their target field descriptor
     */
    private Map<Descriptors.FieldDescriptor, CelRuntime.Program> compileFieldPrograms(Descriptors.Descriptor sourceDescriptor,
                                                                                      Descriptors.Descriptor targetDescriptor,
                                                                                      Map<String, String> protoMapping,
                                                                                      Set<Descriptors.FileDescriptor> fileDescriptors,
                                                                                      CelRuntime celRuntime) {
        Map<Descriptors.FieldDescriptor, CelRuntime.Program> fieldPrograms = new LinkedHashMap<>();
        for (Map.Entry<String, String> mapping : protoMapping.entrySet()) {
            Descriptors.FieldDescriptor fieldDescriptor = targetDescriptor.findFieldByName(mapping.getKey());
            if (fieldDescriptor == null) {
                continue;
            }
            CelAbstractSyntaxTree ast = compileExpression(sourceDescriptor, fieldDescriptor, mapping.getValue(), fileDescriptors);
            fieldPrograms.put(fieldDescriptor, createProgram(celRuntime, ast, fieldDescriptor));
        }
        return fieldPrograms;
    }

    /**
     * Compiles a single CEL expression against the source descriptor and the target field result type.
     *
     * @param sourceDescriptor the descriptor of the source proto message
     * @param fieldDescriptor  the target field the expression populates
     * @param expression       the CEL expression to compile
     * @param fileDescriptors  the proto file descriptors registered with the CEL environment
     * @return the compiled abstract syntax tree
     * @throws ConfigurationException if the expression is invalid or its result type is incompatible with the field
     */
    private CelAbstractSyntaxTree compileExpression(Descriptors.Descriptor sourceDescriptor,
                                                    Descriptors.FieldDescriptor fieldDescriptor,
                                                    String expression,
                                                    Set<Descriptors.FileDescriptor> fileDescriptors) {
        CelCompiler celCompiler = CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                .addFileTypes(fileDescriptors)
                .addVar(ProtoMappingFunction.SOURCE_VARIABLE_NAME, StructTypeReference.create(sourceDescriptor.getFullName()))
                .setResultType(CelTypeMapper.toCelType(fieldDescriptor))
                .build();
        try {
            return celCompiler.compile(expression).getAst();
        } catch (CelValidationException e) {
            throw new ConfigurationException(
                    String.format("invalid CEL expression %s for the field %s: %s",
                            expression, fieldDescriptor.getFullName(), e.getMessage()), e);
        }
    }

    /**
     * Creates an executable CEL program from a compiled expression.
     *
     * @param celRuntime      the CEL runtime used to create the program
     * @param ast             the compiled abstract syntax tree
     * @param fieldDescriptor the target field the program populates
     * @return the executable CEL program
     * @throws ConfigurationException if the program cannot be created
     */
    private CelRuntime.Program createProgram(CelRuntime celRuntime,
                                             CelAbstractSyntaxTree ast,
                                             Descriptors.FieldDescriptor fieldDescriptor) {
        try {
            return celRuntime.createProgram(ast);
        } catch (CelEvaluationException e) {
            throw new ConfigurationException(
                    String.format("failed to create the CEL program for the field %s: %s",
                            fieldDescriptor.getFullName(), e.getMessage()), e);
        }
    }

    /**
     * Collects the transitive set of proto file descriptors reachable from the given message descriptors.
     *
     * @param descriptors the message descriptors to start the traversal from; {@code null} entries are ignored
     * @return the de-duplicated set of file descriptors including their dependencies
     */
    private Set<Descriptors.FileDescriptor> collectFileDescriptors(Descriptors.Descriptor... descriptors) {
        Map<String, Descriptors.FileDescriptor> fileDescriptors = new LinkedHashMap<>();
        for (Descriptors.Descriptor descriptor : descriptors) {
            if (descriptor != null) {
                collectFileDescriptors(descriptor.getFile(), fileDescriptors);
            }
        }
        return new LinkedHashSet<>(fileDescriptors.values());
    }

    /**
     * Recursively collects a file descriptor and its dependencies into the accumulator.
     *
     * @param fileDescriptor  the file descriptor to collect
     * @param fileDescriptors the accumulator keyed by file full name
     */
    private void collectFileDescriptors(Descriptors.FileDescriptor fileDescriptor,
                                        Map<String, Descriptors.FileDescriptor> fileDescriptors) {
        if (fileDescriptors.containsKey(fileDescriptor.getFullName())) {
            return;
        }
        fileDescriptors.put(fileDescriptor.getFullName(), fileDescriptor);
        for (Descriptors.FileDescriptor dependency : fileDescriptor.getDependencies()) {
            collectFileDescriptors(dependency, fileDescriptors);
        }
    }
}
