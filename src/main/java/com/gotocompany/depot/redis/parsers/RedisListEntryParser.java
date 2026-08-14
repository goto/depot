package com.gotocompany.depot.redis.parsers;


import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisListEntry;
import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;

/**
 * Redis list parser.
 *
 * <p>Resolves the key template against the message and reads a single configured field as the value,
 * producing exactly one {@link RedisListEntry} per message to be appended to the list.</p>
 */
@AllArgsConstructor
public class RedisListEntryParser implements RedisEntryParser {
    /**
     * Reporter used to build the {@link Instrumentation} attached to each created entry.
     */
    private final StatsDReporter statsDReporter;
    /**
     * Template that renders the Redis list key from the message.
     */
    private final Template keyTemplate;
    /**
     * Name of the message field whose value is appended to the list.
     */
    private final String field;

    /**
     * Builds the single list entry for the given message.
     *
     * <p>The Redis key is produced by {@link Template#parse(ParsedMessage)} and the value is the
     * string form of the configured field, read via {@link ParsedMessage#getFieldByName(String)}.</p>
     *
     * @param parsedMessage the decoded message to convert
     * @return a singleton list holding one {@link RedisListEntry}
     */
    @Override
    public List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage) {
        String redisKey = keyTemplate.parse(parsedMessage);
        String redisValue = parsedMessage.getFieldByName(field).toString();
        return Collections.singletonList(new RedisListEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisListEntry.class)));
    }
}
