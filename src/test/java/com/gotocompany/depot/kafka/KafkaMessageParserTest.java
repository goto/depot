package com.gotocompany.depot.kafka;

import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.Before;
import org.junit.Test;

public class KafkaMessageParserTest {

    private KafkaMessageParser parser;
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        stencilClient = StencilClientFactory.getClient();
        parser = new KafkaMessageParser(stencilClient,
                "com.gotocompany.depot.TestBookingLogMessage");
    }


    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnNullPayload() throws Exception {
        parser.parse(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnEmptyPayload() throws Exception {
        parser.parse(new byte[0]);
    }
}
