package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.config.converter.JsonToPropertiesConverter;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RedisEntryParserFactory}, which selects and configures a
 * {@link RedisEntryParser} implementation from the {@link RedisSinkConfig}.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked {@link RedisSinkConfig} primed in
 * {@link #setup()} with valid defaults. They verify that each {@link RedisSinkDataType} resolves to
 * its matching parser, and that empty, null or missing data-field and mapping configurations raise an
 * {@link IllegalArgumentException} with a descriptive message.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisEntryParserFactoryTest {
    /**
     * Mocked sink configuration primed with valid defaults in {@link #setup()} and overridden per
     * scenario.
     */
    @Mock
    private RedisSinkConfig redisSinkConfig;
    /**
     * Mocked StatsD reporter passed to the factory.
     */
    @Mock
    private StatsDReporter statsDReporter;

    /**
     * Primes the mocked {@link RedisSinkConfig} with valid defaults — a key template, key-value and
     * list data field names, and a single-entry hash-set field-to-column mapping — so each test only
     * overrides the values relevant to its scenario.
     */
    @Before
    public void setup() {
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("redis-key");
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn("keyvalue-field");
        when(redisSinkConfig.getSinkRedisListDataFieldName()).thenReturn("list-field");
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, "{\"field\":\"column\"}"));
    }

    /**
     * Verifies that the {@code LIST} data type resolves to a list parser.
     *
     * <p>Given the data type stubbed to {@link RedisSinkDataType#LIST}, when
     * {@link RedisEntryParserFactory#getRedisEntryParser} is called, then the returned parser is a
     * {@link RedisListEntryParser}.</p>
     */
    @Test
    public void shouldReturnNewRedisListParser() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        RedisEntryParser parser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        assertEquals(RedisListEntryParser.class, parser.getClass());
    }

    /**
     * Verifies that the {@code HASHSET} data type resolves to a hash-set parser.
     *
     * <p>Given the data type stubbed to {@link RedisSinkDataType#HASHSET}, when
     * {@link RedisEntryParserFactory#getRedisEntryParser} is called, then the returned parser is a
     * {@link RedisHashSetEntryParser}.</p>
     */
    @Test
    public void shouldReturnNewRedisHashSetParser() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        RedisEntryParser parser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        assertEquals(RedisHashSetEntryParser.class, parser.getClass());
    }

    /**
     * Verifies that the {@code KEYVALUE} data type resolves to a key-value parser.
     *
     * <p>Given the data type stubbed to {@link RedisSinkDataType#KEYVALUE}, when
     * {@link RedisEntryParserFactory#getRedisEntryParser} is called, then the returned parser is a
     * {@link RedisKeyValueEntryParser}.</p>
     */
    @Test
    public void shouldReturnNewRedisKeyValueParser() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        RedisEntryParser parser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        assertEquals(RedisKeyValueEntryParser.class, parser.getClass());
    }

    /**
     * Verifies that an empty hash-set field-to-column mapping is rejected.
     *
     * <p>Given the {@code HASHSET} data type with an empty mapping, when
     * {@link RedisEntryParserFactory#getRedisEntryParser} is called, then an
     * {@link IllegalArgumentException} with the message
     * {@code "Empty config SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING found"} is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionForEmptyMappingForHashSet() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, ""));
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter));
        assertEquals("Empty config SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING found", e.getMessage());
    }

    /**
     * Verifies that a null hash-set field-to-column mapping is rejected.
     *
     * <p>Given the {@code HASHSET} data type with a null mapping, when
     * {@link RedisEntryParserFactory#getRedisEntryParser} is called, then an
     * {@link IllegalArgumentException} with the message
     * {@code "Empty config SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING found"} is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionForNullMappingForHashSet() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, null));
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter));
        assertEquals("Empty config SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING found", e.getMessage());
    }

    /**
     * Verifies that a hash-set mapping with an empty template value is rejected.
     *
     * <p>Given the {@code HASHSET} data type with a mapping whose template value is empty (column
     * {@code order_details} mapped to {@code ""}), when
     * {@link RedisEntryParserFactory#getRedisEntryParser} is called, then an
     * {@link IllegalArgumentException} with the message {@code "Template cannot be empty"} is
     * thrown.</p>
     */
    @Test
    public void shouldThrowExceptionForEmptyMappingKeyHashSet() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.HASHSET);
        when(redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping()).thenReturn(new JsonToPropertiesConverter().convert(null, "{\"order_details\":\"\"}"));
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter));
        assertEquals("Template cannot be empty", e.getMessage());
    }

    /**
     * Verifies that an empty key-value data field name is rejected.
     *
     * <p>Given the {@code KEYVALUE} data type with an empty key-value data field name, when
     * {@link RedisEntryParserFactory#getRedisEntryParser} is called, then an
     * {@link IllegalArgumentException} with the message
     * {@code "Empty config SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME found"} is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionForEmptyKeyValueDataFieldName() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn("");
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter));
        assertEquals("Empty config SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME found", illegalArgumentException.getMessage());
    }

    /**
     * Verifies that an empty list data field name is rejected.
     *
     * <p>Given the {@code LIST} data type with an empty list data field name, when
     * {@link RedisEntryParserFactory#getRedisEntryParser} is called, then an
     * {@link IllegalArgumentException} with the message
     * {@code "Empty config SINK_REDIS_LIST_DATA_FIELD_NAME found"} is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionForEmptyListDataFieldName() {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.LIST);
        when(redisSinkConfig.getSinkRedisListDataFieldName()).thenReturn("");
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter));
        assertEquals("Empty config SINK_REDIS_LIST_DATA_FIELD_NAME found", illegalArgumentException.getMessage());
    }

    /**
     * Verifies that an empty key template is rejected before any data-type handling.
     *
     * <p>Given an empty key template, when {@link RedisEntryParserFactory#getRedisEntryParser} is
     * called, then an {@link IllegalArgumentException} with the message
     * {@code "Template cannot be empty"} is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionForEmptyRedisTemplate() {
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("");
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter));
        assertEquals("Template cannot be empty", illegalArgumentException.getMessage());
    }
}
