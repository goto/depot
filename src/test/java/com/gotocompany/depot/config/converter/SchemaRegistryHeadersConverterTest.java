package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.config.SinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link SchemaRegistryHeadersConverter}, exercised end-to-end through the Owner
 * {@link SinkConfig} interface.
 *
 * <p>Rather than calling the converter directly, each test builds a {@link SinkConfig} via
 * {@link ConfigFactory} from a property map and reads
 * {@link SinkConfig#getSchemaRegistryStencilFetchHeaders()}, whose accessor is bound to the
 * converter. The cases cover an empty header value, a completely absent property, and a populated
 * value that is parsed into {@link BasicHeader} objects.
 */
public class SchemaRegistryHeadersConverterTest {
    /**
     * Verifies that an empty {@code SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS} value yields no headers.
     *
     * <p>Given a property map binding {@code SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS} to an empty
     * string, when the {@link SinkConfig} is built and
     * {@link SinkConfig#getSchemaRegistryStencilFetchHeaders()} is read, then the returned list is
     * empty.
     */
    @Test
    public void testConvertIfFetchHeadersValueEmpty() {
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS", "");
            }
        };
        SinkConfig config = ConfigFactory.create(SinkConfig.class, properties);
        Assert.assertEquals(0, config.getSchemaRegistryStencilFetchHeaders().size());
    }

    /**
     * Verifies that an absent header property yields no headers.
     *
     * <p>Given an empty property map, when the {@link SinkConfig} is built and
     * {@link SinkConfig#getSchemaRegistryStencilFetchHeaders()} is read, then the returned list is
     * empty.
     */
    @Test
    public void shouldReturnZeroIfPropertyNotMentioned() {
        Map<String, String> properties = new HashMap<String, String>() {
        };
        SinkConfig config = ConfigFactory.create(SinkConfig.class, properties);
        Assert.assertEquals(0, config.getSchemaRegistryStencilFetchHeaders().size());
    }

    /**
     * Verifies that a populated, loosely formatted header value is parsed into ordered
     * {@link BasicHeader} objects.
     *
     * <p>Given {@code SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS} set to
     * {@code "key1:value1 ,,, key2 : value2,"} (with blank segments and surrounding whitespace),
     * when {@link SinkConfig#getSchemaRegistryStencilFetchHeaders()} is read, then the blank
     * segments are skipped and the result is a two-element list equal to the {@link BasicHeader}
     * {@code key1:value1} followed by {@code key2:value2}.
     */
    @Test
    public void shouldConvertHeaderKeyValuesWithHeaderObject() {
        Map<String, String> properties = new HashMap<String, String>() {
            {
                put("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS", "key1:value1 ,,, key2 : value2,");
            }
        };
        SinkConfig config = ConfigFactory.create(SinkConfig.class, properties);
        Assert.assertEquals((new BasicHeader("key1", "value1")).toString(), config.getSchemaRegistryStencilFetchHeaders().get(0).toString());
        Assert.assertEquals((new BasicHeader("key2", "value2")).toString(), config.getSchemaRegistryStencilFetchHeaders().get(1).toString());
        Assert.assertEquals(2, config.getSchemaRegistryStencilFetchHeaders().size());
    }
}
