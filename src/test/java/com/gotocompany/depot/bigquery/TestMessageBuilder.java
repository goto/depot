package com.gotocompany.depot.bigquery;

import com.google.api.client.util.DateTime;
import com.gotocompany.depot.TestKeyBQ;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.message.Message;

import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


/**
 * Test fixture builder that assembles {@link Message} instances and their expected metadata columns
 * for the BigQuery sink tests.
 *
 * <p>A builder is seeded from a {@link TestMetadata} record through
 * {@link #withMetadata(TestMetadata)} and then produces {@link Message} objects whose key and value
 * are serialized {@link TestKeyBQ} and {@link TestMessageBQ} protobufs, carrying the topic,
 * partition, offset, timestamp and load time as message metadata tuples. The static
 * {@link #metadataColumns(TestMetadata, Instant)} helper builds the matching map of expected BigQuery
 * metadata columns so tests can assert against the converted output.</p>
 */
public final class TestMessageBuilder {
    /** Event timestamp applied to messages built by this instance, in epoch milliseconds. */
    private long timestamp;
    /** Source topic applied to messages built by this instance. */
    private String topic;
    /** Partition index applied to messages built by this instance. */
    private int partition;
    /** Offset applied to messages built by this instance. */
    private long offset;
    /** Load (ingestion) time applied to messages built by this instance, in epoch milliseconds. */
    private long loadTime;

    /**
     * Creates an empty builder; obtain a seeded instance through {@link #withMetadata(TestMetadata)}.
     */
    private TestMessageBuilder() {
    }

    /**
     * Creates a builder pre-populated from the supplied test metadata.
     *
     * @param testMetadata the metadata whose topic, partition, offset, timestamp and load time seed
     *                      the builder
     * @return a new {@link TestMessageBuilder} carrying the supplied metadata values
     */
    public static TestMessageBuilder withMetadata(TestMetadata testMetadata) {
        TestMessageBuilder builder = new TestMessageBuilder();
        builder.topic = testMetadata.getTopic();
        builder.partition = testMetadata.getPartition();
        builder.offset = testMetadata.getOffset();
        builder.timestamp = testMetadata.getTimestamp();
        builder.loadTime = testMetadata.getLoadTime();
        return builder;
    }

    /**
     * Builds a {@link Message} populated with both a key and a value from the given order fields.
     *
     * <p>The key is a serialized {@link TestKeyBQ} (order number and URL) and the value is a
     * serialized {@link TestMessageBQ} (order number, URL and details). The builder's metadata is
     * attached as message tuples, including an extra {@code should_be_ignored} tuple used to verify
     * that unmapped metadata is dropped during conversion.</p>
     *
     * @param orderNumber  the order number set on both the key and the value
     * @param orderUrl     the order URL set on both the key and the value
     * @param orderDetails the order details set on the value
     * @return a {@link Message} carrying the serialized key, value and metadata tuples
     */
    public Message createConsumerRecord(String orderNumber, String orderUrl, String orderDetails) {
        TestKeyBQ key = TestKeyBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .setOrderDetails(orderDetails)
                .build();
        return new Message(
                key.toByteArray(),
                message.toByteArray(),
                new Tuple<>("message_topic", topic),
                new Tuple<>("message_partition", partition),
                new Tuple<>("message_offset", offset),
                new Tuple<>("message_timestamp", timestamp),
                new Tuple<>("load_time", loadTime),
                new Tuple<>("should_be_ignored", timestamp));
    }

    /**
     * Builds a {@link Message} that carries a key but a {@code null} value.
     *
     * <p>Used to exercise the converter's handling of records whose payload is absent: the key is a
     * serialized {@link TestKeyBQ} while the value bytes are {@code null}, and the builder's metadata
     * tuples are attached as usual.</p>
     *
     * @param orderNumber the order number set on the key
     * @param orderUrl    the order URL set on the key
     * @return a {@link Message} with a populated key, a {@code null} value and metadata tuples
     */
    public Message createEmptyValueConsumerRecord(String orderNumber, String orderUrl) {
        TestKeyBQ key = TestKeyBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        return new Message(
                key.toByteArray(),
                null,
                new Tuple<>("message_topic", topic),
                new Tuple<>("message_partition", partition),
                new Tuple<>("message_offset", offset),
                new Tuple<>("message_timestamp", timestamp),
                new Tuple<>("load_time", loadTime),
                new Tuple<>("should_be_ignored", timestamp));
    }

    /**
     * Builds the map of expected BigQuery metadata columns for the supplied record metadata.
     *
     * <p>The partition, offset and topic are copied verbatim, while the message timestamp and the
     * load time (taken from {@code now}) are wrapped in {@link DateTime} values to mirror how the
     * converter renders timestamp columns. Tests use the returned map as the expected metadata
     * portion of a converted record.</p>
     *
     * @param testMetadata the source metadata to project into columns
     * @param now          the instant used as the {@code load_time} column value
     * @return a map of metadata column names to their expected values
     */
    public static Map<String, Object> metadataColumns(TestMetadata testMetadata, Instant now) {
        Map<String, Object> metadataColumns = new HashMap<>();
        metadataColumns.put("message_partition", testMetadata.getPartition());
        metadataColumns.put("message_offset", testMetadata.getOffset());
        metadataColumns.put("message_topic", testMetadata.getTopic());
        metadataColumns.put("message_timestamp", new DateTime(testMetadata.getTimestamp()));
        metadataColumns.put("load_time", new DateTime(Date.from(now)));
        return metadataColumns;
    }
}
