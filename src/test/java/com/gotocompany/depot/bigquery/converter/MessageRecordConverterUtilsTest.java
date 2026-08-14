package com.gotocompany.depot.bigquery.converter;

import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.Message;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link MessageRecordConverterUtils}, the helper that augments BigQuery column maps
 * with message metadata and JSON event timestamps.
 *
 * <p>The tests drive the static helpers with a mocked {@link Message} and configurations created via
 * {@code ConfigFactory}, asserting that the configured metadata columns are merged into the target
 * column map and that an {@code event_timestamp} column is injected for JSON sources when
 * enabled.</p>
 */
public class MessageRecordConverterUtilsTest {

    /**
     * Verifies that configured metadata columns are merged into the existing column map.
     *
     * <p>Given a column map with one entry, a {@link Message} whose metadata exposes three values and
     * a config enabling metadata with matching column types, when {@code addMetadata} runs, then the
     * three metadata entries are added alongside the original column.</p>
     */
    @Test
    public void shouldAddMetaData() {
        Map<String, Object> columns = new HashMap<String, Object>() {{
            put("test", 123);
        }};
        Message message = Mockito.mock(Message.class);
        Mockito.when(message.getMetadata(Mockito.any())).thenReturn(new HashMap<String, Object>() {{
            put("test2", "value2");
            put("something", 99L);
            put("nvm", "nvm");
        }});
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, new HashMap<String, String>() {{
            put("SINK_BIGQUERY_ADD_METADATA_ENABLED", "true");
            put("SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "test2=string,something=long,nvm=string");
        }});
        MessageRecordConverterUtils.addMetadata(columns, message, config);
        Assert.assertEquals(new HashMap<String, Object>() {{
            put("test", 123);
            put("test2", "value2");
            put("something", 99L);
            put("nvm", "nvm");
        }}, columns);
    }

    /**
     * Verifies that an event-timestamp column is injected for JSON sources when enabled.
     *
     * <p>Given a column map with one entry and a JSON config with event-timestamp injection enabled,
     * when {@code addTimeStampColumnForJson} runs, then the map gains a non-null
     * {@code event_timestamp} column, growing to two entries.</p>
     */
    @Test
    public void shouldAddTimeStampForJson() {
        Map<String, Object> columns = new HashMap<String, Object>() {{
            put("test", 123);
        }};
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, new HashMap<String, String>() {{
            put("SINK_CONNECTOR_SCHEMA_DATA_TYPE", "json");
            put("SINK_BIGQUERY_ADD_EVENT_TIMESTAMP_ENABLE", "true");
        }});
        MessageRecordConverterUtils.addTimeStampColumnForJson(columns, config);
        Assert.assertEquals(2, columns.size());
        Assert.assertNotNull(columns.get("event_timestamp"));
    }
}
