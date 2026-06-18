package com.gotocompany.depot.redis.parsers;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.config.converter.JsonToPropertiesConverter;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestKey;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisHashSetFieldEntry;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.IllegalFormatConversionException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the hash-set Redis entry parser produced by {@link RedisEntryParserFactory}, which
 * maps a {@link ParsedMessage} to {@link RedisHashSetFieldEntry} instances for the {@code HASHSET}
 * sink type using a field-to-column mapping plus templated keys and fields.
 *
 * <p>The tests run under {@link MockitoJUnitRunner}. Some scenarios build a real
 * {@link RedisSinkConfig} via {@link ConfigFactory} to exercise complex protobuf shapes (repeated
 * nested messages and {@link Struct} values), while others use the {@link #redisSinkSetup(String)}
 * fixture with a mocked configuration to vary the field-to-column mapping. They cover successful
 * templating of keys and fields, parsing from both the message body and the message key, and the
 * exceptions raised for invalid or type-incompatible templates.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisHashSetEntryParserTest {
    /**
     * Mocked sink configuration used by the {@link #redisSinkSetup(String)} scenarios.
     */
    @Mock
    private RedisSinkConfig redisSinkConfig;
    /**
     * Mocked StatsD reporter passed to the proto parser and the entry-parser factory.
     */
    @Mock
    private StatsDReporter statsDReporter;
    /**
     * Parsed {@link TestBookingLogMessage} body used as the parser input in most scenarios.
     */
    private ParsedMessage parsedBookingMessage;
    /**
     * Parsed {@link TestKey} ({@code LOG_KEY} mode) used to verify key-sourced parsing.
     */
    private ParsedMessage parsedKey;

    /**
     * Stubs the mocked {@link RedisSinkConfig} for a {@code HASHSET} sink keyed by {@code "test-key"}
     * with the given field-to-column mapping, then parses a fixed {@link TestBookingLogMessage} both as
     * the message body ({@code parsedBookingMessage}) and, via its key bytes, as a {@link TestKey}
     * ({@code parsedKey}).
     *
     * @param field the JSON field-to-column mapping to stub for
     *     {@code SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING}
     * @throws IOException if parsing a fixture message fails
     */
    private void redisSinkSetup(String field) throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, field));
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("test-key");
        String schemaBookingClass = "com.gotocompany.depot.TestBookingLogMessage";
        String schemaKeyClass = "com.gotocompany.depot.TestKey";
        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("booking-order-1").setCustomerTotalFareWithoutSurge(2000L).setAmountPaidByCash(12.3F).build();
        Message bookingMessage = new Message(testKey.toByteArray(), testBookingLogMessage.toByteArray());
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(redisSinkConfig, statsDReporter, null);
        parsedBookingMessage = protoMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaBookingClass);
        parsedKey = protoMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_KEY, schemaKeyClass);
    }

    /**
     * Verifies that a repeated nested message field is serialised to JSON with templated key and field.
     *
     * <p>Given a {@code HASHSET} configuration mapping {@code topics} to
     * {@code "topics_%s,customer_name"} with key template
     * {@code "subscription:driver:%s,customer_name"} and a {@link TestBookingLogMessage} carrying
     * customer {@code "johndoe"} and two topic entries, when {@link RedisEntryParser#getRedisEntry} is
     * called, then it returns a single {@link RedisHashSetFieldEntry} with key
     * {@code "subscription:driver:johndoe"}, field {@code "topics_johndoe"} and a JSON-array value of
     * the two topics.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldParseComplexProtoType() throws IOException {
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_DATA_TYPE", "HASHSET",
                "SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING", "{\"topics\":\"topics_%s,customer_name\"}",
                "SINK_REDIS_KEY_TEMPLATE", "subscription:driver:%s,customer_name"
        ));
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(config, statsDReporter, null);
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setCustomerName("johndoe")
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(1)
                        .setTopic("hellowo/rl/dcom.world.partner").build())
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(123)
                        .setTopic("topic2").build())
                .build();
        Message bookingMessage = new Message(null, testBookingLogMessage.toByteArray());
        String schemaMessageClass = "com.gotocompany.depot.TestBookingLogMessage";

        parsedBookingMessage = protoMessageParser.parse(bookingMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaMessageClass);

        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(config, statsDReporter);
        List<RedisEntry> redisEntry = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        assertEquals(1, redisEntry.size());
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisEntry.get(0);
        assertEquals("subscription:driver:johndoe", redisHashSetFieldEntry.getKey());
        assertEquals("topics_johndoe", redisHashSetFieldEntry.getField());
        assertEquals(new JSONArray("[{\"qos\":1,\"topic\":\"hellowo/rl/dcom.world.partner\"},{\"qos\":123,\"topic\":\"topic2\"}]").toString(),
                new JSONArray(redisHashSetFieldEntry.getValue()).toString());
    }

    /**
     * Verifies that a repeated {@link Struct} field is serialised to JSON with a timestamp-formatted
     * field name.
     *
     * <p>Given a {@code HASHSET} configuration mapping {@code attributes} to
     * {@code "test_order_%s,created_at"} with key template
     * {@code "subscription:order:%s,order_number"} and a {@link TestMessageBQ} carrying three
     * {@link Struct} attributes, a creation timestamp and order number {@code "test_order"}, when
     * {@link RedisEntryParser#getRedisEntry} is called, then it returns a single
     * {@link RedisHashSetFieldEntry} with key {@code "subscription:order:test_order"}, field
     * {@code "test_order_2022-11-26T03:29:19Z"} and the attributes as a JSON-array value.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldParseRepeatedStruct() throws IOException {
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_DATA_TYPE", "HASHSET",
                "SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING", "{\"attributes\":\"test_order_%s,created_at\"}",
                "SINK_REDIS_KEY_TEMPLATE", "subscription:order:%s,order_number"
        ));
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(config, statsDReporter, null);
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(50).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(60).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("active", Value.newBuilder().setBoolValue(true).build())
                        .putFields("height", Value.newBuilder().setNumberValue(175).build()).build())
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1669433359).build())
                .setOrderNumber("test_order")
                .build();

        Message message1 = new Message(null, message.toByteArray());
        ParsedMessage parsedMessage = protoMessageParser.parse(message1, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessageBQ");

        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(config, statsDReporter);
        List<RedisEntry> redisEntry = redisHashSetEntryParser.getRedisEntry(parsedMessage);
        assertEquals(1, redisEntry.size());
        RedisHashSetFieldEntry redisHashSetFieldEntry = (RedisHashSetFieldEntry) redisEntry.get(0);
        assertEquals("subscription:order:test_order", redisHashSetFieldEntry.getKey());
        assertEquals("test_order_2022-11-26T03:29:19Z", redisHashSetFieldEntry.getField());
        assertEquals("[{\"name\":\"John\",\"age\":50},{\"name\":\"John\",\"age\":60},{\"name\":\"John\",\"active\":true,\"height\":175}]",
                redisHashSetFieldEntry.getValue());
    }

    /**
     * Verifies that a mapping referencing a non-existent column is rejected.
     *
     * <p>Given a {@code HASHSET} configuration mapping the unknown column {@code does_not_exist}, when
     * {@link RedisEntryParser#getRedisEntry} is called for a {@link TestMessageBQ}, then an
     * {@link IllegalArgumentException} with the message
     * {@code "Invalid field config : does_not_exist"} is thrown.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldThrowExceptionForWrongConfig() throws IOException {
        RedisSinkConfig config = ConfigFactory.create(RedisSinkConfig.class, ImmutableMap.of(
                "SINK_REDIS_DATA_TYPE", "HASHSET",
                "SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING", "{\"does_not_exist\":\"test_order_%s,order_number\"}",
                "SINK_REDIS_KEY_TEMPLATE", "subscription:order:%s,order_number"
        ));

        ProtoMessageParser protoMessageParser = new ProtoMessageParser(config, statsDReporter, null);
        TestMessageBQ message = TestMessageBQ.newBuilder().setOrderNumber("test").build();
        Message message1 = new Message(null, message.toByteArray());
        ParsedMessage parsedMessage = protoMessageParser.parse(message1, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessageBQ");

        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(config, statsDReporter);
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> redisHashSetEntryParser.getRedisEntry(parsedMessage));
        Assert.assertEquals("Invalid field config : does_not_exist", exception.getMessage());
    }

    /**
     * Verifies that a long-typed column is formatted into the hash field template.
     *
     * <p>Given a {@code HASHSET} mapping that sources the entry value from {@code order_number} and
     * builds the hash field from template {@code "ORDER_NUMBER_%s,customer_total_fare_without_surge"},
     * where the booking message's fare is the long {@code 2000}, when
     * {@link RedisEntryParser#getRedisEntry} is called, then it returns a single
     * {@link RedisHashSetFieldEntry} of key {@code "test-key"}, field {@code "ORDER_NUMBER_2000"} and
     * value {@code "booking-order-1"}.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldParseLongMessageForKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER_%s,customer_total_fare_without_surge\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER_2000", "booking-order-1", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }

    /**
     * Verifies that whitespace around the templated column name is tolerated.
     *
     * <p>Given the same long-fare scenario but with a space after the comma in the field template
     * ({@code "ORDER_NUMBER_%s, customer_total_fare_without_surge"}), when
     * {@link RedisEntryParser#getRedisEntry} is called, then the produced
     * {@link RedisHashSetFieldEntry} is identical (key {@code "test-key"}, field
     * {@code "ORDER_NUMBER_2000"}, value {@code "booking-order-1"}), confirming the column name is
     * trimmed.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldParseLongMessageWithSpaceForKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER_%s, customer_total_fare_without_surge\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER_2000", "booking-order-1", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }

    /**
     * Verifies that a string column is interpolated into the hash field template.
     *
     * <p>Given a {@code HASHSET} mapping that sources both the value and the field-template
     * substitution from {@code order_number} (template {@code "ORDER_NUMBER_%s,order_number"}), when
     * {@link RedisEntryParser#getRedisEntry} is called, then it returns a single
     * {@link RedisHashSetFieldEntry} of key {@code "test-key"}, field
     * {@code "ORDER_NUMBER_booking-order-1"} and value {@code "booking-order-1"}.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldParseStringMessageForKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER_%s,order_number\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER_booking-order-1", "booking-order-1", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }

    /**
     * Verifies that a static field template with no placeholder is used verbatim.
     *
     * <p>Given a {@code HASHSET} mapping whose field template is the literal {@code "ORDER_NUMBER"}
     * with the value sourced from {@code order_number}, when {@link RedisEntryParser#getRedisEntry} is
     * called, then it returns a single {@link RedisHashSetFieldEntry} of key {@code "test-key"}, field
     * {@code "ORDER_NUMBER"} and value {@code "booking-order-1"}.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldHandleStaticStringForKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedBookingMessage);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER", "booking-order-1", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }

    /**
     * Verifies that a malformed field template is rejected when the parser is built.
     *
     * <p>Given a {@code HASHSET} mapping with the malformed template
     * {@code "ORDER_NUMBER%, order_number"}, when
     * {@link RedisEntryParserFactory#getRedisEntryParser} is called, then an
     * {@link IllegalArgumentException} with the message
     * {@code "Template is not valid, variables=1, validArgs=0, values=1"} is thrown.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldThrowErrorForInvalidFormatForKey() throws IOException {
        redisSinkSetup("{\"order_details\":\"ORDER_NUMBER%, order_number\"}");
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class, () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter));
        assertEquals("Template is not valid, variables=1, validArgs=0, values=1", e.getMessage());
    }

    /**
     * Verifies that a format specifier incompatible with the column type fails at parse time.
     *
     * <p>Given a {@code HASHSET} mapping with template {@code "order_number-%d, order_number"} that
     * applies the numeric {@code %d} specifier to the string {@code order_number} column, when
     * {@link RedisEntryParser#getRedisEntry} is called, then an
     * {@link IllegalFormatConversionException} with the message {@code "d != java.lang.String"} is
     * thrown.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldThrowErrorForIncompatibleFormatForKey() throws IOException {
        redisSinkSetup("{\"order_details\":\"order_number-%d, order_number\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        IllegalFormatConversionException e = Assert.assertThrows(IllegalFormatConversionException.class,
                () -> redisHashSetEntryParser.getRedisEntry(parsedBookingMessage));
        assertEquals("d != java.lang.String", e.getMessage());
    }

    /**
     * Verifies that the parser reads values from a parsed message key.
     *
     * <p>Given a static field template {@code "ORDER_NUMBER"} sourcing its value from
     * {@code order_number} and the {@link TestKey} parsed in {@code LOG_KEY} mode (order number
     * {@code "ORDER-1-FROM-KEY"}), when {@link RedisEntryParser#getRedisEntry} is called with that
     * parsed key, then it returns a single {@link RedisHashSetFieldEntry} of key {@code "test-key"},
     * field {@code "ORDER_NUMBER"} and value {@code "ORDER-1-FROM-KEY"}.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldParseKeyWhenKafkaMessageParseModeSetToKey() throws IOException {
        redisSinkSetup("{\"order_number\":\"ORDER_NUMBER\"}");
        RedisEntryParser redisHashSetEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        List<RedisEntry> redisEntries = redisHashSetEntryParser.getRedisEntry(parsedKey);
        RedisHashSetFieldEntry expectedEntry = new RedisHashSetFieldEntry("test-key", "ORDER_NUMBER", "ORDER-1-FROM-KEY", null);
        assertEquals(Collections.singletonList(expectedEntry), redisEntries);
    }
}
