package com.gotocompany.depot.message.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import com.gotocompany.depot.utils.StencilUtils;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import com.jayway.jsonpath.Configuration;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
public class ProtoMessageParser implements MessageParser {

    private final StencilClient stencilClient;
    private final ProtoFieldParser protoMappingParser = new ProtoFieldParser();
    private final Configuration jsonPathConfig;

    public ProtoMessageParser(SinkConfig sinkConfig, StatsDReporter reporter, DepotStencilUpdateListener protoUpdateListener) {

        StencilConfig stencilConfig = StencilUtils.getStencilConfig(sinkConfig, reporter.getClient(), protoUpdateListener);
        if (sinkConfig.isSchemaRegistryStencilEnable()) {
            stencilClient = StencilClientFactory.getClient(sinkConfig.getSchemaRegistryStencilUrls(), stencilConfig);
        } else {
            stencilClient = StencilClientFactory.getClient();
        }

        jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
    }

    public ProtoMessageParser(StencilClient stencilClient, Configuration jsonPathConfig) {
        this.jsonPathConfig = jsonPathConfig;
        this.stencilClient = stencilClient;
    }

    public ParsedMessage parse(Message message, SinkConnectorSchemaMessageMode type, String schemaClass) throws IOException {
        if (type == null) {
            throw new IOException("parser mode not defined");
        }
        MessageUtils.validate(message, byte[].class);
        byte[] payload;
        switch (type) {
            case LOG_MESSAGE:
                payload = (byte[]) message.getLogMessage();
                break;
            case LOG_KEY:
                payload = (byte[]) message.getLogKey();
                break;
            default:
                throw new ConfigurationException("Schema type not supported");
        }
        if (payload == null || payload.length == 0) {
            log.info("empty message found {}", message.getMetadataString());
            throw new EmptyMessageException();
        }
        DynamicMessage dynamicMessage = stencilClient.parse(schemaClass, payload);
        return new ProtoParsedMessage(dynamicMessage, jsonPathConfig);
    }

    public Map<String, Descriptors.Descriptor> getDescriptorMap() {
        return stencilClient.getAll();
    }

    @Override
    public void refresh(String schemaClass) {
        // this will try to fetch the descriptor for the class and reloading the cache in the process
        // this is useful for batch ingestion with frequency more than TTL and possibility of new data.
        stencilClient.get(schemaClass);
    }

    private Map<String, String> getTypeNameToPackageNameMap(Map<String, Descriptors.Descriptor> descriptors) {
        return descriptors.entrySet().stream()
                .filter(distinctByFullName(t -> t.getValue().getFullName()))
                .collect(Collectors.toMap(
                        (mapEntry) -> String.format(".%s", mapEntry.getValue().getFullName()),
                        Map.Entry::getKey));
    }

    private <T> Predicate<T> distinctByFullName(Function<? super T, Object> keyExtractor) {
        Set<Object> objects = new HashSet<>();
        return t -> objects.add(keyExtractor.apply(t));
    }

    public ProtoField getProtoField(String schemaClass, Map<String, Descriptors.Descriptor> newDescriptors) throws IOException {
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, schemaClass, newDescriptors,
                getTypeNameToPackageNameMap(newDescriptors));
        return protoField;
    }

    public ProtoField getProtoField(String schemaClass) throws IOException {
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, schemaClass, getDescriptorMap(),
                getTypeNameToPackageNameMap(getDescriptorMap()));
        return protoField;
    }

    public Descriptors.Descriptor getDescriptor(String schemaClass) {
        return stencilClient.get(schemaClass);
    }
}
