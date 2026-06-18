package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisKeyValueEntry;
import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;

/**
 * {@link RedisEntryParser} for the {@code KEYVALUE} data type.
 *
 * <p>Resolves the key template against the message and reads a single configured field as the value,
 * producing exactly one {@link RedisKeyValueEntry} per message.</p>
 */
@AllArgsConstructor
public class RedisKeyValueEntryParser implements RedisEntryParser {
    /**
     * Reporter used to build the {@link Instrumentation} attached to each created entry.
     */
    private final StatsDReporter statsDReporter;
    /**
     * Template that renders the Redis key from the message.
     */
    private final Template keyTemplate;
    /**
     * Name of the message field whose value is stored.
     */
    private final String fieldName;

    /**
     * Builds the single key/value entry for the given message.
     *
     * <p>The Redis key is produced by {@link Template#parse(ParsedMessage)} and the value is the
     * string form of the field named by {@code fieldName}, read via
     * {@link ParsedMessage#getFieldByName(String)}.</p>
     *
     * @param parsedMessage the decoded message to convert
     * @return a singleton list holding one {@link RedisKeyValueEntry}
     */
    @Override
    public List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage) {
        String redisKey = keyTemplate.parse(parsedMessage);
        String redisValue = parsedMessage.getFieldByName(fieldName).toString();
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisKeyValueEntry.class));
        return Collections.singletonList(redisKeyValueEntry);
    }
}
