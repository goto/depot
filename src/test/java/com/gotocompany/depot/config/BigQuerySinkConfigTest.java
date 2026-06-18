package com.gotocompany.depot.config;

import com.gotocompany.depot.common.TupleString;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * Unit tests for {@link BigQuerySinkConfig}, the Owner-based configuration interface for the
 * BigQuery sink.
 *
 * <p>The test builds a {@link BigQuerySinkConfig} from system properties via {@link ConfigFactory}
 * and asserts that a typed accessor parses its raw property value as expected.
 */
public class BigQuerySinkConfigTest {

    /**
     * Verifies that {@link BigQuerySinkConfig#getMetadataColumnsTypes()} parses the configured
     * metadata column type mapping into ordered tuples.
     *
     * <p>Given {@code SINK_BIGQUERY_METADATA_COLUMNS_TYPES} set to
     * {@code "topic=string,partition=integer,offset=integer"} (with the proto message class and
     * auto-schema-update properties also configured), when the config is built and
     * {@link BigQuerySinkConfig#getMetadataColumnsTypes()} is read, then the result is a list of
     * three {@link TupleString} entries: {@code (topic, string)}, {@code (partition, integer)} and
     * {@code (offset, integer)}.
     */
    @Test
    public void testMetadataTypes() {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestKeyBQ");
        System.setProperty("SINK_BIGQUERY_ENABLE_AUTO_SCHEMA_UPDATE", "false");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "topic=string,partition=integer,offset=integer");
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
        Assert.assertEquals(new ArrayList<TupleString>() {{
            add(new TupleString("topic", "string"));
            add(new TupleString("partition", "integer"));
            add(new TupleString("offset", "integer"));
        }}, metadataColumnsTypes);
    }
}
