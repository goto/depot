package com.gotocompany.depot.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * Static helpers for inspecting Protobuf message values.
 *
 * <p>These utilities support schema validation and field-type checks used while converting Protobuf
 * records. The central capability is detecting unknown fields — fields present in the wire data but
 * absent from the compiled schema — by walking a {@link com.google.protobuf.Message} tree, with the
 * breadth of that walk controlled by a
 * {@link com.gotocompany.depot.message.ProtoUnknownFieldValidationType}. Additional helpers classify
 * an individual {@link com.google.protobuf.Descriptors.FieldDescriptor}.</p>
 */
public class ProtoUtils {
    /**
     * Determines whether a Protobuf message, or any nested message it contains, carries unknown
     * fields.
     *
     * <p>The message tree rooted at {@code root} is flattened into the set of messages selected by the
     * given {@link com.gotocompany.depot.message.ProtoUnknownFieldValidationType} (which governs how
     * repeated message fields are descended into), and each is checked for Protobuf unknown fields. The
     * message is considered to have unknown fields as soon as any inspected node reports at least
     * one.</p>
     *
     * @param root the root Protobuf message to inspect
     * @param protoUnknownFieldValidationType the strategy controlling how the message tree is traversed
     *     when collecting candidate messages
     * @return {@code true} if any inspected message contains unknown fields, {@code false} otherwise
     */
    public static boolean hasUnknownField(Message root, ProtoUnknownFieldValidationType protoUnknownFieldValidationType) {
        List<Message> messageFields = collectNestedFields(root, protoUnknownFieldValidationType);
        List<Message> messageWithUnknownFields = getMessageWithUnknownFields(messageFields);
        return !messageWithUnknownFields.isEmpty();

    }

    /**
     * Flattens a Protobuf message tree into a list containing the root and all descendant messages
     * selected by the validation strategy.
     *
     * <p>Performs an iterative, depth-first traversal using an explicit LIFO queue to avoid recursion.
     * Starting from {@code node}, each message's set fields are filtered and expanded through the
     * supplied {@link com.gotocompany.depot.message.ProtoUnknownFieldValidationType} to obtain the
     * child messages to descend into; every visited message is added to the output. The strategy
     * decides whether, and how many of, the elements of a repeated message field are followed.</p>
     *
     * @param node the root message at which traversal begins
     * @param protoUnknownFieldValidationType the strategy that filters and maps field values to the
     *     child messages to visit
     * @return a list of every message visited during the traversal, including {@code node}
     */
    private static List<Message> collectNestedFields(Message node, ProtoUnknownFieldValidationType protoUnknownFieldValidationType) {
        List<Message> output = new LinkedList<>();
        Queue<Message> stack = Collections.asLifoQueue(new LinkedList<>());
        stack.add(node);
        while (true) {
            Message current = stack.poll();
            if (current == null) {
                break;
            }
            List<Message> nestedChildNodes = current.getAllFields().values().stream()
                    .filter(protoUnknownFieldValidationType::shouldFilter)
                    .flatMap(protoUnknownFieldValidationType::getMapper)
                    .collect(Collectors.toList());
            stack.addAll(nestedChildNodes);

            output.add(current);
        }

        return output;
    }

    /**
     * Filters a list of messages down to those that carry Protobuf unknown fields.
     *
     * @param messages the messages to examine
     * @return the subset of {@code messages} whose unknown-field set is non-empty
     */
    private static List<Message> getMessageWithUnknownFields(List<Message> messages) {
        return messages.stream().filter(message -> !message.getUnknownFields().asMap().isEmpty()).collect(Collectors.toList());
    }

    /**
     * Reports whether a field is a singular (non-repeated) Protobuf message field.
     *
     * @param fieldDescriptor the field descriptor to classify
     * @return {@code true} if the field is not repeated and its type is
     *     {@link com.google.protobuf.Descriptors.FieldDescriptor.Type#MESSAGE}, {@code false}
     *     otherwise
     */
    public static boolean isNonRepeatedProtoMessage(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.isRepeated()) {
            return false;
        }
        return Descriptors.FieldDescriptor.Type.MESSAGE == fieldDescriptor.getType();
    }

    /**
     * Reports whether a field is a singular (non-repeated) string field.
     *
     * @param fieldDescriptor the field descriptor to classify
     * @return {@code true} if the field is not repeated and its type is
     *     {@link com.google.protobuf.Descriptors.FieldDescriptor.Type#STRING}, {@code false} otherwise
     */
    public static boolean isNonRepeatedString(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.isRepeated()) {
            return false;
        }
        return Descriptors.FieldDescriptor.Type.STRING == fieldDescriptor.getType();
    }

}
