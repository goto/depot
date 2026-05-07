package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;

import java.util.HashMap;
import java.util.Map;

public class ProtoMappingFunction {

    private final Map<String, CelRuntime.Program> compiledPrograms;
    private final String sourceBindingName;
    private final Descriptors.Descriptor sinkMessageDescriptor;
    private final Descriptors.Descriptor sinkKeyDescriptor;

    public ProtoMappingFunction(
            Map<String, CelRuntime.Program> compiledPrograms,
            String sourceBindingName,
            Descriptors.Descriptor sinkMessageDescriptor,
            Descriptors.Descriptor sinkKeyDescriptor) {
        this.compiledPrograms = compiledPrograms;
        this.sourceBindingName = sourceBindingName;
        this.sinkMessageDescriptor = sinkMessageDescriptor;
        this.sinkKeyDescriptor = sinkKeyDescriptor;
    }

    public MappedMessages map(DynamicMessage sourceMessage) throws CelEvaluationException {
        Map<String, Object> bindings = new HashMap<>();
        bindings.put(sourceBindingName, sourceMessage);

        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(sinkMessageDescriptor);
        DynamicMessage.Builder keyBuilder = sinkKeyDescriptor != null
                ? DynamicMessage.newBuilder(sinkKeyDescriptor) : null;

        Map<String, Descriptors.FieldDescriptor> messageFields = new HashMap<>();
        for (Descriptors.FieldDescriptor fd : sinkMessageDescriptor.getFields()) {
            messageFields.put(fd.getName(), fd);
        }

        Map<String, Descriptors.FieldDescriptor> keyFields = new HashMap<>();
        if (sinkKeyDescriptor != null) {
            for (Descriptors.FieldDescriptor fd : sinkKeyDescriptor.getFields()) {
                keyFields.put(fd.getName(), fd);
            }
        }

        for (Map.Entry<String, CelRuntime.Program> entry : compiledPrograms.entrySet()) {
            String outputField = entry.getKey();
            CelRuntime.Program program = entry.getValue();
            Object result = program.eval(bindings);

            if (messageFields.containsKey(outputField)) {
                messageBuilder.setField(messageFields.get(outputField), result);
            }
            if (keyFields.containsKey(outputField)) {
                keyBuilder.setField(keyFields.get(outputField), result);
            }
        }

        DynamicMessage outputMessage = messageBuilder.build();
        DynamicMessage outputKey = keyBuilder != null ? keyBuilder.build() : null;
        return new MappedMessages(outputMessage, outputKey);
    }

    public static class MappedMessages {
        private final DynamicMessage message;
        private final DynamicMessage key;

        public MappedMessages(DynamicMessage message, DynamicMessage key) {
            this.message = message;
            this.key = key;
        }

        public DynamicMessage getMessage() {
            return message;
        }

        public DynamicMessage getKey() {
            return key;
        }
    }
}
