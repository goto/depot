package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.message.ParsedMessage;

import java.util.List;

/**
 * Converts a single {@link ParsedMessage} into the {@link RedisEntry} objects to write for it.
 *
 * <p>Each implementation corresponds to one
 * {@link com.gotocompany.depot.redis.enums.RedisSinkDataType} and knows how to resolve the configured
 * key (and, for hash sets, field) templates against the message and to extract the value(s).
 * Implementations are created by {@link RedisEntryParserFactory}. A key/value or list message yields
 * exactly one entry, whereas a hash set message may yield several (one per configured field).</p>
 *
 * @see RedisKeyValueEntryParser
 * @see RedisListEntryParser
 * @see RedisHashSetEntryParser
 */
public interface RedisEntryParser {

    /**
     * Builds the Redis entries to write for the given parsed message.
     *
     * @param parsedMessage the decoded message to convert
     * @return the list of {@link RedisEntry} objects to write; typically a single entry, or one per
     *     configured field for hash sets
     */
    List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage);
}
