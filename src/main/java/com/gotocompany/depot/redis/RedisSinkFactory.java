package com.gotocompany.depot.redis;


import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.client.RedisClient;
import com.gotocompany.depot.redis.client.RedisClientFactory;
import com.gotocompany.depot.redis.parsers.RedisEntryParser;
import com.gotocompany.depot.redis.parsers.RedisEntryParserFactory;
import com.gotocompany.depot.redis.parsers.RedisParser;
import com.gotocompany.depot.utils.MessageConfigUtils;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;

/**
 * Factory that wires together and builds {@link RedisSink} instances from a {@link RedisSinkConfig}.
 *
 * <p>The factory is constructed once, and {@link #init()} is called a single time to validate the
 * configuration and build the shared, reusable {@link RedisParser} (the message parser plus the
 * data-type specific {@link RedisEntryParser}). {@link #create()} is then invoked once per worker to
 * produce a ready-to-use {@link Sink}; each call builds a fresh, non-thread-safe {@link RedisClient}
 * so that every worker owns an independent Jedis connection.</p>
 *
 * @see RedisSink
 * @see RedisClientFactory
 * @see RedisEntryParserFactory
 */
public class RedisSinkFactory {
    /**
     * Configuration describing the Redis deployment, key template, data type and TTL settings.
     */
    private final RedisSinkConfig sinkConfig;
    /**
     * Reporter used to build {@link Instrumentation} for the factory, sink and client.
     */
    private final StatsDReporter statsDReporter;
    /**
     * Shared parser built once by {@link #init()} and reused by every {@link RedisSink} created.
     */
    private RedisParser redisParser;

    /**
     * Creates a factory that emits metrics through the supplied reporter.
     *
     * @param sinkConfig the Redis sink configuration
     * @param statsDReporter the StatsD reporter used for instrumentation
     */
    public RedisSinkFactory(RedisSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    /**
     * Creates a factory that discards all metrics.
     *
     * <p>Instrumentation is backed by a {@link NoOpStatsDClient}, so every metric emission is silently
     * dropped. This overload is convenient for tests or deployments that do not collect StatsD
     * metrics.</p>
     *
     * @param sinkConfig the Redis sink configuration
     */
    public RedisSinkFactory(RedisSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = new StatsDReporter(new NoOpStatsDClient());
    }

    /**
     * Validates the configuration and builds the reusable {@link RedisParser} shared by every sink.
     *
     * <p>The method logs a summary of the resolved Redis configuration (urls, key template, data
     * type, deployment type and TTL settings), augmented with the field configuration specific to the
     * configured data type ({@code LIST}, {@code KEYVALUE} or {@code HASHSET}). It then builds the
     * {@link MessageParser} via {@link MessageParserFactory}, resolves the message mode and schema
     * with {@link MessageConfigUtils}, creates the data-type specific {@link RedisEntryParser} through
     * {@link RedisEntryParserFactory}, and combines them into the {@link RedisParser} stored on this
     * factory.</p>
     *
     * @throws IllegalArgumentException if any exception occurs while building the parser; the original
     *     exception is preserved as the cause
     */
    public void init() {
        try {
            Instrumentation instrumentation = new Instrumentation(statsDReporter, RedisSinkFactory.class);
            String redisConfig = String.format("\n\tredis.urls = %s\n\tredis.key.template = %s\n\tredis.sink.data.type = %s"
                            + "\n\tredis.deployment.type = %s\n\tredis.ttl.type = %s\n\tredis.ttl.value = %d\n\t",
                    sinkConfig.getSinkRedisUrls(),
                    sinkConfig.getSinkRedisKeyTemplate(),
                    sinkConfig.getSinkRedisDataType().toString(),
                    sinkConfig.getSinkRedisDeploymentType().toString(),
                    sinkConfig.getSinkRedisTtlType().toString(),
                    sinkConfig.getSinkRedisTtlValue());
            switch (sinkConfig.getSinkRedisDataType()) {
                case LIST:
                    redisConfig += "redis.list.data.field.name=" + sinkConfig.getSinkRedisListDataFieldName();
                    break;
                case KEYVALUE:
                    redisConfig += "redis.keyvalue.data.field.name=" + sinkConfig.getSinkRedisKeyValueDataFieldName();
                    break;
                case HASHSET:
                    redisConfig += "redis.hashset.field.to.column.mapping=" + sinkConfig.getSinkRedisHashsetFieldToColumnMapping().toString();
                    break;
                default:
            }
            instrumentation.logInfo(redisConfig);
            instrumentation.logInfo("Redis server type = {}", sinkConfig.getSinkRedisDeploymentType());
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter);
            Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
            RedisEntryParser redisEntryParser = RedisEntryParserFactory.getRedisEntryParser(sinkConfig, statsDReporter);
            this.redisParser = new RedisParser(messageParser, redisEntryParser, modeAndSchema);
            instrumentation.logInfo("Connection to redis established successfully");
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception occurred while creating Redis sink", e);
        }
    }

    /**
     * We create redis client for each create call, because it's not thread safe.
     *
     * <p>A new {@link RedisClient} is created for every call because the underlying Jedis client is
     * not thread safe; each returned sink therefore owns an independent connection. The client is
     * initialised via {@link RedisClient#init()} before being wrapped, together with the shared
     * {@link RedisParser} built in {@link #init()}, into a new {@link RedisSink}.</p>
     *
     * @return RedisSink
     */
    public Sink create() {
        RedisClient redisClient = RedisClientFactory.getClient(sinkConfig, statsDReporter);
        redisClient.init();
        return new RedisSink(
                redisClient,
                redisParser,
                new Instrumentation(statsDReporter, RedisSink.class));
    }
}
