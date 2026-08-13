package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.gotocompany.depot.exception.ProtoMappingException;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;

import java.util.Collections;
import java.util.Map;

/**
 * Builds a single sink proto message by evaluating the per-field CEL programs against a source message.
 */
public class ProtoMessageMapper {

    private final Descriptors.Descriptor targetDescriptor;
    private final Map<Descriptors.FieldDescriptor, CelRuntime.Program> fieldPrograms;
    private final CelValueConverter celValueConverter;

    /**
     * Creates a message mapper for a single target proto type.
     *
     * @param targetDescriptor  the descriptor of the proto message to build
     * @param fieldPrograms     the compiled CEL programs keyed by their target field descriptor
     * @param celValueConverter the converter that adapts CEL values to proto field values
     */
    public ProtoMessageMapper(Descriptors.Descriptor targetDescriptor,
                              Map<Descriptors.FieldDescriptor, CelRuntime.Program> fieldPrograms,
                              CelValueConverter celValueConverter) {
        this.targetDescriptor = targetDescriptor;
        this.fieldPrograms = fieldPrograms;
        this.celValueConverter = celValueConverter;
    }

    /**
     * Builds the target proto message by evaluating every configured field program against the source message.
     *
     * <p>Fields whose mapping evaluates to a null value are left unset and therefore keep their proto default value.
     *
     * @param sourceMessage the parsed source proto message
     * @return the mapped proto message
     * @throws ProtoMappingException if a mapping expression fails to evaluate or produces an incompatible value
     */
    public DynamicMessage map(Message sourceMessage) {
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(targetDescriptor);
        for (Map.Entry<Descriptors.FieldDescriptor, CelRuntime.Program> fieldProgram : fieldPrograms.entrySet()) {
            Descriptors.FieldDescriptor fieldDescriptor = fieldProgram.getKey();
            Object fieldValue = celValueConverter.toFieldValue(evaluate(fieldProgram.getValue(), fieldDescriptor, sourceMessage), fieldDescriptor);
            if (fieldValue != null) {
                messageBuilder.setField(fieldDescriptor, fieldValue);
            }
        }
        return messageBuilder.build();
    }

    /**
     * Evaluates a single field program against the source message.
     *
     * @param program         the compiled CEL program for the field
     * @param fieldDescriptor the target field the program populates
     * @param sourceMessage   the parsed source proto message bound to the CEL source variable
     * @return the raw CEL evaluation result
     * @throws ProtoMappingException if the CEL program fails to evaluate
     */
    private Object evaluate(CelRuntime.Program program, Descriptors.FieldDescriptor fieldDescriptor, Message sourceMessage) {
        try {
            return program.eval(Collections.singletonMap(ProtoMappingFunction.SOURCE_VARIABLE_NAME, sourceMessage));
        } catch (CelEvaluationException e) {
            throw new ProtoMappingException(
                    String.format("failed to evaluate the mapping expression for the field %s: %s",
                            fieldDescriptor.getFullName(), e.getMessage()), e);
        }
    }
}
