package com.gotocompany.depot.message;

import com.google.protobuf.Message;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * This class provides utility methods to check if a protobuf message has unknown fields.
 * This class is used in conjunction with ProtoUtils.hasUnknownField method to check if a protobuf message has unknown fields.
 *
 * Three types of validation are supported:
 * 1. MESSAGE: Checks if the given object is a protobuf message that contains unknown fields.
 * 2. MESSAGE_ARRAY_FIRST_INDEX: Checks if the given object is a protobuf message or a list of protobuf messages that contains unknown fields only on the first element.
 * 3. MESSAGE_ARRAY_FULL: Checks if the given object is a protobuf message or a list of protobuf messages that contains unknown fields.
 *
 * <p>This enum supplies the per-strategy behavior used by
 * {@link com.gotocompany.depot.utils.ProtoUtils#hasUnknownField} when traversing a message tree. Each
 * constant decides, through {@link #shouldFilter(Object)}, whether a given object should be descended
 * into, and, through {@link #getMapper(Object)}, which Protobuf {@link Message} instances that object
 * contributes to the traversal. Together they control how aggressively repeated message fields are
 * examined.</p>
 *
 * <p>Three strategies are supported:</p>
 * <ul>
 *   <li>{@link #MESSAGE}: treats only a singular Protobuf message as a candidate, ignoring lists
 *   entirely.</li>
 *   <li>{@link #MESSAGE_ARRAY_FIRST_INDEX}: treats a singular message or a list of messages as a
 *   candidate but, for a list, inspects only the first element.</li>
 *   <li>{@link #MESSAGE_ARRAY_FULL}: treats a singular message or a list of messages as a candidate
 *   and inspects every element of the list.</li>
 * </ul>
 *
 * @see com.gotocompany.depot.utils.ProtoUtils#hasUnknownField
 */
public enum ProtoUnknownFieldValidationType {
    /**
     * Validates only a singular Protobuf message, ignoring any list values.
     */
    MESSAGE {
        /**
         * Accepts the candidate only when it is itself a Protobuf {@link Message}.
         *
         * @param object the candidate field value to test
         * @return {@code true} if {@code object} is a {@link Message}, {@code false} otherwise
         */
        @Override
        public boolean shouldFilter(Object object) {
            return object instanceof Message;
        }

        /**
         * Wraps the given message in a single-element stream.
         *
         * @param object the message to inspect, known to be a {@link Message}
         * @return a stream containing {@code object} cast to {@link Message}
         */
        @Override
        public Stream<Message> getMapper(Object object) {
            return Stream.of((Message) object);
        }
    },
    /**
     * Validates a singular message, or only the first element of a list of messages.
     */
    MESSAGE_ARRAY_FIRST_INDEX {
        /**
         * Accepts a singular message or a non-empty list whose first element is a message.
         *
         * <p>The decision is delegated to the enum's {@code isMessageOrMessageListType} helper.</p>
         *
         * @param object the candidate field value to test
         * @return {@code true} if {@code object} is a message or a list of messages
         */
        @Override
        public boolean shouldFilter(Object object) {
            return isMessageOrMessageListType(object);
        }

        /**
         * Streams at most one message to inspect from the candidate.
         *
         * <p>A singular {@link Message} is returned directly. For a {@link List}, only the first
         * element is considered and it is emitted when it passes this strategy's {@code shouldFilter}
         * check; otherwise the stream is empty. Any other type yields an empty stream.</p>
         *
         * @param object the candidate message or list of messages
         * @return a stream containing at most one message to inspect
         */
        @Override
        public Stream<Message> getMapper(Object object) {
            if (object instanceof Message) {
                return Stream.of((Message) object);
            }
            if (object instanceof List) {
                return Optional.ofNullable(((List<?>) object).get(0))
                        .filter(this::shouldFilter)
                        .map(o -> Stream.of((Message) o))
                        .orElseGet(Stream::empty);
            }
            return Stream.empty();
        }
    },
    /**
     * Validates a singular message, or every element of a list of messages.
     */
    MESSAGE_ARRAY_FULL {
        /**
         * Accepts a singular message or a non-empty list whose first element is a message.
         *
         * <p>The decision is delegated to the enum's {@code isMessageOrMessageListType} helper.</p>
         *
         * @param object the candidate field value to test
         * @return {@code true} if {@code object} is a message or a list of messages
         */
        @Override
        public boolean shouldFilter(Object object) {
            return isMessageOrMessageListType(object);
        }

        /**
         * Streams every message to inspect from the candidate.
         *
         * <p>A singular {@link Message} is returned directly. A {@link List} that passes this
         * strategy's {@code shouldFilter} check has all of its elements streamed as messages. Any
         * other type yields an empty stream.</p>
         *
         * @param object the candidate message or list of messages
         * @return a stream of all messages to inspect, possibly empty
         */
        @Override
        public Stream<Message> getMapper(Object object) {
            if (object instanceof Message) {
                return Stream.of((Message) object);
            }
            if (object instanceof List) {
                return Optional.of(object)
                        .filter(this::shouldFilter)
                        .map(messageList -> ((List<Message>) messageList).stream())
                        .orElseGet(Stream::empty);
            }
            return Stream.empty();
        }
    };

    /**
     * Determines whether the given object is a candidate to be inspected for unknown fields.
     *
     * <p>Implementations return {@code true} only for the object shapes this strategy knows how to
     * descend into; the traversal uses this as a filter before {@link #getMapper(Object)} is
     * applied.</p>
     *
     * @param object the candidate field value to test; may be {@code null}
     * @return {@code true} if {@code object} should be considered for unknown-field inspection,
     *     {@code false} otherwise
     */
    public abstract boolean shouldFilter(Object object);

    /**
     * Expands a candidate object into the stream of Protobuf messages it contributes to the
     * traversal.
     *
     * <p>Depending on the strategy this yields the object itself when it is a message, selected
     * elements of a list of messages, or an empty stream when the object holds nothing to
     * inspect.</p>
     *
     * @param object the candidate object previously accepted by {@link #shouldFilter(Object)}
     * @return a stream of the Protobuf {@link Message} instances to inspect, possibly empty
     */
    public abstract Stream<Message> getMapper(Object object);

    /**
     * Determines whether an object is a Protobuf {@link Message} or a non-empty {@link List} of
     * messages.
     *
     * <p>Returns {@code false} for {@code null}. A bare {@link Message} returns {@code true}. A
     * {@link List} returns {@code true} only when it is non-empty and its first element is a
     * {@link Message}; the remaining elements are not examined. All other types return
     * {@code false}.</p>
     *
     * @param object the object to classify; may be {@code null}
     * @return {@code true} if {@code object} is a message or a list whose first element is a message
     */
    private static boolean isMessageOrMessageListType(Object object) {
        if (Objects.isNull(object)) {
            return false;
        }
        if (object instanceof Message) {
            return true;
        }
        if (object instanceof List) {
            List<Object> list = (List) object;

            if (list.isEmpty()) {
                return false;
            }
            return list.get(0) instanceof Message;
        }
        return false;
    }
}
