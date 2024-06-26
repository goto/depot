package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.config.SinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryHeadersConverterTest {
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

    @Test
    public void shouldReturnZeroIfPropertyNotMentioned() {
        Map<String, String> properties = new HashMap<String, String>() {
        };
        SinkConfig config = ConfigFactory.create(SinkConfig.class, properties);
        Assert.assertEquals(0, config.getSchemaRegistryStencilFetchHeaders().size());
    }

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
