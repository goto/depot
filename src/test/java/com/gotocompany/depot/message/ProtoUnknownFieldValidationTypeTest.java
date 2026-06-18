package com.gotocompany.depot.message;

import com.google.protobuf.Message;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Unit tests for {@link ProtoUnknownFieldValidationType}, the enum strategy that decides which values
 * are inspected for unknown Protobuf fields.
 *
 * <p>The tests use Mockito to stand in for Protobuf {@link Message} instances and exercise the two
 * strategy hooks on each constant: {@link ProtoUnknownFieldValidationType#shouldFilter(Object)},
 * which selects candidate values, and {@link ProtoUnknownFieldValidationType#getMapper(Object)},
 * which expands a candidate into the stream of messages to inspect. Coverage spans the
 * {@code MESSAGE}, {@code MESSAGE_ARRAY_FIRST_INDEX} and {@code MESSAGE_ARRAY_FULL} constants across
 * singular messages, lists of messages, non-message values, empty lists and {@code null}.</p>
 */
public class ProtoUnknownFieldValidationTypeTest {

    /**
     * Verifies that the {@code MESSAGE} strategy accepts a singular Protobuf message.
     *
     * <p>Given the {@link ProtoUnknownFieldValidationType#MESSAGE} strategy and a mocked Protobuf
     * {@link Message}, when {@code shouldFilter} is called, then it returns {@code true}.</p>
     */
    @Test
    public void shouldFilterMessageType() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE;
        Message message = Mockito.mock(Message.class);

        Assertions.assertTrue(type.shouldFilter(message));
    }

    /**
     * Verifies that the {@code MESSAGE} strategy rejects a non-message value.
     *
     * <p>Given the {@code MESSAGE} strategy and an {@link Integer}, when {@code shouldFilter} is
     * called, then it returns {@code false}.</p>
     */
    @Test
    public void shouldNotFilterNonMessageType() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE;
        Integer message = 2;

        Assertions.assertFalse(type.shouldFilter(message));
    }

    /**
     * Verifies that the {@code MESSAGE} strategy maps a message to a single-element stream.
     *
     * <p>Given the {@code MESSAGE} strategy and a mocked Protobuf {@link Message}, when
     * {@code getMapper} is called, then the resulting stream contains exactly that message.</p>
     */
    @Test
    public void shouldReturnStreamOfMessage() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE;
        Message message = Mockito.mock(Message.class);

        Assertions.assertEquals(type.getMapper(message).collect(Collectors.toList()),
                Stream.of(message).collect(Collectors.toList()));
    }

    /**
     * Verifies that {@code MESSAGE_ARRAY_FIRST_INDEX} accepts both a message and a list of messages.
     *
     * <p>Given the {@link ProtoUnknownFieldValidationType#MESSAGE_ARRAY_FIRST_INDEX} strategy, when
     * {@code shouldFilter} is called with a singular message and with a singleton list of messages,
     * then it returns {@code true} in both cases.</p>
     */
    @Test
    public void shouldFilterMessageOrMessageListType() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Message message = Mockito.mock(com.google.protobuf.Message.class);
        List<Message> messageList = Collections.singletonList(message);

        Assertions.assertTrue(type.shouldFilter(message));
        Assertions.assertTrue(type.shouldFilter(messageList));
    }

    /**
     * Verifies that the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy rejects a non-message value.
     *
     * <p>Given the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy and an {@link Integer}, when
     * {@code shouldFilter} is called, then it returns {@code false}.</p>
     */
    @Test
    public void shouldFilterOutNonMessageType() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Integer message = 2;

        Assertions.assertFalse(type.shouldFilter(message));
    }

    /**
     * Verifies that the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy rejects an empty list.
     *
     * <p>Given the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy and an empty list, when
     * {@code shouldFilter} is called, then it returns {@code false}.</p>
     */
    @Test
    public void shouldFilterOutEmptyMessageList() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        List<Message> messageList = new ArrayList<>();

        Assertions.assertFalse(type.shouldFilter(messageList));
    }

    /**
     * Verifies that the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy rejects {@code null}.
     *
     * <p>Given the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy and a {@code null} value, when
     * {@code shouldFilter} is called, then it returns {@code false}.</p>
     */
    @Test
    public void shouldFilterOutNullObject() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;

        Assertions.assertFalse(type.shouldFilter(null));
    }

    /**
     * Verifies that {@code MESSAGE_ARRAY_FIRST_INDEX} maps a singular message to a single-element stream.
     *
     * <p>Given the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy and a mocked Protobuf {@link Message},
     * when {@code getMapper} is called, then the resulting stream contains exactly that message.</p>
     */
    @Test
    public void shouldMapToSingularStreamOfMessage() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Message message = Mockito.mock(Message.class);

        List<Message> result = type.getMapper(message).collect(Collectors.toList());

        Assertions.assertEquals(Stream.of(message).collect(Collectors.toList()), result);
    }

    /**
     * Verifies that the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy maps only the first list element.
     *
     * <p>Given the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy and a list of two messages, when
     * {@code getMapper} is called, then the resulting stream contains only the first message.</p>
     */
    @Test
    public void shouldMapToStreamOfMessagesOnlyFirstIndex() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Message message1 = Mockito.mock(Message.class);
        Message message2 = Mockito.mock(Message.class);
        List<Message> objects = new ArrayList<>();
        objects.add(message1);
        objects.add(message2);

        List<Message> result = type.getMapper(objects).collect(Collectors.toList());

        Assertions.assertEquals(Stream.of(message1).collect(Collectors.toList()), result);
    }

    /**
     * Verifies that {@code MESSAGE_ARRAY_FIRST_INDEX} maps a non-message value to an empty stream.
     *
     * <p>Given the {@code MESSAGE_ARRAY_FIRST_INDEX} strategy and an {@link Integer}, when
     * {@code getMapper} is called, then the resulting stream is empty.</p>
     */
    @Test
    public void shouldMapToEmptyStreamWhenNonMessageListIsGiven() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FIRST_INDEX;
        Integer message = 2;

        List<Message> result = type.getMapper(message).collect(Collectors.toList());

        Assertions.assertEquals(Collections.emptyList(), result);
    }

    /**
     * Verifies that the {@code MESSAGE_ARRAY_FULL} strategy accepts a list of messages.
     *
     * <p>Given the {@link ProtoUnknownFieldValidationType#MESSAGE_ARRAY_FULL} strategy and a list of
     * two messages, when {@code shouldFilter} is called, then it returns {@code true}.</p>
     */
    @Test
    public void shouldFilterFullArrayMessage() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FULL;
        Message message1 = Mockito.mock(Message.class);
        Message message2 = Mockito.mock(Message.class);
        List<Message> objects = new ArrayList<>();
        objects.add(message1);
        objects.add(message2);

        Assertions.assertTrue(type.shouldFilter(objects));
    }

    /**
     * Verifies that the {@code MESSAGE_ARRAY_FULL} strategy maps every list element.
     *
     * <p>Given the {@code MESSAGE_ARRAY_FULL} strategy and a list of two messages, when
     * {@code getMapper} is called, then the resulting stream contains all of the list's messages.</p>
     */
    @Test
    public void shouldMapFullArrayToMessage() {
        ProtoUnknownFieldValidationType type = ProtoUnknownFieldValidationType.MESSAGE_ARRAY_FULL;
        Message message1 = Mockito.mock(Message.class);
        Message message2 = Mockito.mock(Message.class);
        List<Message> objects = new ArrayList<>();
        objects.add(message1);
        objects.add(message2);

        List<Message> result = type.getMapper(objects).collect(Collectors.toList());

        Assertions.assertEquals(objects, result);
    }
}
