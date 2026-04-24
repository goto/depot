package com.gotocompany.depot.kafka;

import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkTest {

    @Mock
    private KafkaSinkConfig config;

    @Mock
    private Instrumentation instrumentation;

    private KafkaSink kafkaSink;

    @Before
    public void setUp() {
        when(config.getSinkKafkaTopic()).thenReturn("output-topic");
        kafkaSink = new KafkaSink(config, instrumentation);
    }

    @Test
    public void shouldReturnEmptyResponseForEmptyMessages() {
        List<Message> messages = new ArrayList<>();
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertNotNull(response);
        Assert.assertFalse(response.hasErrors());
    }

    @Test
    public void shouldReturnResponseWithoutErrorsForMessages() {
        List<Message> messages = new ArrayList<>();
        messages.add(new Message("key1".getBytes(), "value1".getBytes()));
        messages.add(new Message("key2".getBytes(), "value2".getBytes()));
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertNotNull(response);
        Assert.assertFalse(response.hasErrors());
    }

    @Test
    public void shouldNotThrowOnClose() throws Exception {
        kafkaSink.close();
    }
}
