package com.gotocompany.depot.kafka;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaMessageSerializerTest {

    private KafkaMessageSerializer serializer;

    @Before
    public void setUp() {
        serializer = new KafkaMessageSerializer();
    }

    @Test
    public void shouldReturnEmptyBytesForNullMessage() {
        byte[] result = serializer.serialize(null);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.length);
    }

    @Test
    public void shouldSerializeEmptyMessage() throws Exception {
        StencilClient stencilClient = StencilClientFactory.getClient();
        DynamicMessage emptyMessage = DynamicMessage.newBuilder(
                stencilClient.get("com.gotocompany.depot.TestMessage")).build();

        byte[] result = serializer.serialize(emptyMessage);
        Assert.assertNotNull(result);
    }
}
