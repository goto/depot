package com.gotocompany.depot.kafka;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;

public class KafkaSink implements Sink {

    private final KafkaSinkConfig config;
    private final Instrumentation instrumentation;

    public KafkaSink(KafkaSinkConfig config, Instrumentation instrumentation) {
        this.config = config;
        this.instrumentation = instrumentation;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) {
        SinkResponse response = new SinkResponse();
        instrumentation.logInfo("Pushing {} messages to Kafka sink topic {}", messages.size(), config.getSinkKafkaTopic());
        return response;
    }

    @Override
    public void close() throws IOException {
    }
}
