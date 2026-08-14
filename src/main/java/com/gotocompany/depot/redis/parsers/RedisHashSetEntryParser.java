package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisHashSetFieldEntry;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Redis hash set parser.
 *
 * <p>Resolves the shared key template against the message and then, for every entry in the configured
 * field-to-column mapping, renders the field name from its template and reads the corresponding column
 * value from the message. This produces one {@link RedisHashSetFieldEntry} per configured field, all
 * sharing the same hash key.</p>
 */
@AllArgsConstructor
public class RedisHashSetEntryParser implements RedisEntryParser {
    /**
     * Reporter used to build the {@link Instrumentation} attached to each created entry.
     */
    private final StatsDReporter statsDReporter;
    /**
     * Template that renders the shared Redis hash key from the message.
     */
    private final Template keyTemplate;
    /**
     * Mapping from message field name (column) to the template that renders the hash field name.
     */
    private final Map<String, Template> fieldTemplates;

    /**
     * Builds one hash-field entry per configured field for the given message.
     *
     * <p>The shared hash key is produced once by {@link Template#parse(ParsedMessage)}. For each entry
     * of {@code fieldTemplates} the hash field name is rendered from its {@link Template} and the
     * value is the string form of the column named by the entry key, read via
     * {@link ParsedMessage#getFieldByName(String)}.</p>
     *
     * @param parsedMessage the decoded message to convert
     * @return a list holding one {@link RedisHashSetFieldEntry} for each configured field
     */
    @Override
    public List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage) {
        String redisKey = keyTemplate.parse(parsedMessage);
        return fieldTemplates
                .entrySet()
                .stream()
                .map(fieldTemplate -> {
                    String field = fieldTemplate.getValue().parse(parsedMessage);
                    String redisValue = parsedMessage.getFieldByName(fieldTemplate.getKey()).toString();
                    return new RedisHashSetFieldEntry(redisKey, field, redisValue, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
                }).collect(Collectors.toList());
    }
}
