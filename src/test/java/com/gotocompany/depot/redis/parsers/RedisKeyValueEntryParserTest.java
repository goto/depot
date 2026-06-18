package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisKeyValueEntry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the key-value-data-type Redis entry parser produced by
 * {@link RedisEntryParserFactory}, which maps a {@link ParsedMessage} to a {@link RedisKeyValueEntry}
 * for the {@code KEYVALUE} sink type.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked {@link RedisSinkConfig}. Each
 * scenario calls {@link #redisSinkSetup(String, String)} to stub the configuration, parse a
 * {@link TestMessage} through a real {@link ProtoMessageParser} and resolve the parser under test.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisKeyValueEntryParserTest {
    /**
     * Mocked sink configuration stubbed for the {@code KEYVALUE} data type per scenario.
     */
    @Mock
    private RedisSinkConfig redisSinkConfig;
    /**
     * Mocked StatsD reporter passed to the proto parser and the entry-parser factory.
     */
    @Mock
    private StatsDReporter statsDReporter;
    /**
     * Parser under test, resolved from the factory in {@link #redisSinkSetup(String, String)}.
     */
    private RedisEntryParser redisKeyValueEntryParser;
    /**
     * Parsed fixture message fed to the parser under test.
     */
    private ParsedMessage parsedMessage;

    /**
     * Stubs the {@link RedisSinkConfig} for a {@code KEYVALUE} sink with the given key template and
     * value data field, parses a fixed {@link TestMessage} through a real {@link ProtoMessageParser},
     * and resolves the parser under test from {@link RedisEntryParserFactory}.
     *
     * @param template the Redis key template to stub
     * @param field the key-value data field name to stub
     * @throws IOException if parsing the fixture message fails
     */
    private void redisSinkSetup(String template, String field) throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn(field);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn(template);
        ProtoMessageParser messageParser = new ProtoMessageParser(redisSinkConfig, statsDReporter, null);
        String schemaClass = "com.gotocompany.depot.TestMessage";
        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("new-eureka-order")
                .build()
                .toByteArray();
        Message message = new Message(null, logMessage);
        parsedMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass);
        redisKeyValueEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
    }

    /**
     * Verifies that a valid field configuration yields the expected key-value entry.
     *
     * <p>Given a {@code KEYVALUE} setup keyed by {@code "test-key"} reading field
     * {@code order_details}, when {@link RedisEntryParser#getRedisEntry} is called with the parsed
     * message, then it returns a single {@link RedisKeyValueEntry} mapping {@code "test-key"} to
     * {@code "new-eureka-order"}.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldConvertParsedMessageToRedisKeyValueEntry() throws IOException {
        redisSinkSetup("test-key", "order_details");
        List<RedisEntry> redisDataEntries = redisKeyValueEntryParser.getRedisEntry(parsedMessage);
        RedisKeyValueEntry expectedEntry = new RedisKeyValueEntry("test-key", "new-eureka-order", null);
        assertEquals(Collections.singletonList(expectedEntry), redisDataEntries);
    }

    /**
     * Verifies that an unknown key-value data field is rejected.
     *
     * <p>Given a {@code KEYVALUE} setup whose data field is {@code "random-field"}, when
     * {@link RedisEntryParser#getRedisEntry} is called, then an {@link IllegalArgumentException} with
     * the message {@code "Invalid field config : random-field"} is thrown.</p>
     *
     * @throws IOException if parsing the fixture message fails
     */
    @Test
    public void shouldThrowExceptionForInvalidKeyValueDataFieldName() throws IOException {
        redisSinkSetup("test-key", "random-field");
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> redisKeyValueEntryParser.getRedisEntry(parsedMessage));
        assertEquals("Invalid field config : random-field", exception.getMessage());
    }
}
