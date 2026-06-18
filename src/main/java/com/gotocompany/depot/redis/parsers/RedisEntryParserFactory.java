package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.metrics.StatsDReporter;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Redis parser factory.
 *
 * <p>The key template is compiled once from {@code SINK_REDIS_KEY_TEMPLATE} and shared by the parser.
 * Depending on {@link RedisSinkConfig#getSinkRedisDataType()} the factory returns a
 * {@link RedisKeyValueEntryParser}, a {@link RedisListEntryParser} or (the default) a
 * {@link RedisHashSetEntryParser}, validating that the data-type specific configuration required by
 * each is present.</p>
 */
public class RedisEntryParserFactory {

    /**
     * Creates the {@link RedisEntryParser} for the configured Redis data type.
     *
     * <p>The configured key template is compiled into a {@link Template} (a failure is rethrown as an
     * {@link IllegalArgumentException}). Then, based on
     * {@link RedisSinkConfig#getSinkRedisDataType()}:</p>
     * <ul>
     *   <li>{@code KEYVALUE} requires {@code SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME} and yields a
     *       {@link RedisKeyValueEntryParser};</li>
     *   <li>{@code LIST} requires {@code SINK_REDIS_LIST_DATA_FIELD_NAME} and yields a
     *       {@link RedisListEntryParser};</li>
     *   <li>otherwise ({@code HASHSET}) {@code SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING} is required
     *       and each mapping value is compiled into a field {@link Template}, yielding a
     *       {@link RedisHashSetEntryParser}.</li>
     * </ul>
     *
     * @param redisSinkConfig the Redis sink configuration
     * @param statsDReporter the reporter passed to the created parser for instrumentation
     * @return the {@link RedisEntryParser} matching the configured data type
     * @throws IllegalArgumentException if the key or a field template is invalid, or if the
     *     configuration required by the selected data type is missing or empty
     */
    public static RedisEntryParser getRedisEntryParser(
            RedisSinkConfig redisSinkConfig,
            StatsDReporter statsDReporter) {
        Template keyTemplate;
        try {
            keyTemplate = new Template(redisSinkConfig.getSinkRedisKeyTemplate());
        } catch (InvalidTemplateException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        switch (redisSinkConfig.getSinkRedisDataType()) {
            case KEYVALUE:
                String fieldName = redisSinkConfig.getSinkRedisKeyValueDataFieldName();
                if (fieldName == null || fieldName.isEmpty()) {
                    throw new IllegalArgumentException("Empty config SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME found");
                }
                return new RedisKeyValueEntryParser(statsDReporter, keyTemplate, fieldName);
            case LIST:
                String field = redisSinkConfig.getSinkRedisListDataFieldName();
                if (field == null || field.isEmpty()) {
                    throw new IllegalArgumentException("Empty config SINK_REDIS_LIST_DATA_FIELD_NAME found");
                }
                return new RedisListEntryParser(statsDReporter, keyTemplate, field);
            default:
                Properties properties = redisSinkConfig.getSinkRedisHashsetFieldToColumnMapping();
                if (properties == null || properties.isEmpty()) {
                    throw new IllegalArgumentException("Empty config SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING found");
                }

                Map<String, Template> fieldTemplates = properties.entrySet().stream().collect(Collectors.toMap(
                        kv -> kv.getKey().toString(), kv -> {
                            try {
                                return new Template(kv.getValue().toString());
                            } catch (InvalidTemplateException e) {
                                throw new IllegalArgumentException(e.getMessage());
                            }
                        }
                ));
                return new RedisHashSetEntryParser(statsDReporter, keyTemplate, fieldTemplates);
        }
    }
}
