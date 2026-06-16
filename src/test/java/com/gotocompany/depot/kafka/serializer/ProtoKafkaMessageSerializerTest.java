package com.gotocompany.depot.kafka.serializer;

import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.exception.DeserializerException;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

public class ProtoKafkaMessageSerializerTest {

    @Test
    public void shouldSerializeProtoMessage() {
        TestMessage message = TestMessage.newBuilder().setOrderNumber("order-1").build();
        assertArrayEquals(message.toByteArray(), new ProtoKafkaMessageSerializer().serialize(message));
    }

    @Test
    public void shouldThrowForNullMessage() {
        assertThrows(DeserializerException.class, () -> new ProtoKafkaMessageSerializer().serialize(null));
    }
}
