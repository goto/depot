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

/**
 * {@link MessageParser} implementation that deserializes Protobuf-encoded payloads into
 * {@link ProtoParsedMessage} instances using a Stencil schema-registry client.
 *
 * <p>Descriptors are resolved through a {@link StencilClient}, which may be backed either by a remote
 * Stencil schema registry or by the local classpath depending on the supplied {@link SinkConfig}.
 * Each parsed message is paired with a JSONPath {@link Configuration} whose JSON provider is a
 * {@link ProtoJsonProvider}, enabling later field extraction by name. Beyond parsing, the class can
 * build the nested {@link ProtoField} schema tree for a class and expose the full descriptor map,
 * which supports schema-aware sinks and column mapping.</p>
 *
 * <p>The class is annotated with Lombok's {@code @Slf4j}; empty payloads are logged at info level
 * before an exception is raised.</p>
 *
 * @see MessageParser
 * @see ProtoParsedMessage
 * @see ProtoFieldParser
 */
@Slf4j
public class ProtoMessageParser implements MessageParser {

    /**
     * Stencil client used to resolve Protobuf descriptors and to decode payloads into dynamic
     * messages.
     */
    private final StencilClient stencilClient;
    /**
     * Parser that walks descriptors to build the nested {@link ProtoField} schema tree.
     */
    private final ProtoFieldParser protoMappingParser = new ProtoFieldParser();
    /**
     * JSONPath configuration backed by a {@link ProtoJsonProvider}, propagated to every parsed
     * message so that fields can be looked up by name.
     */
    private final Configuration jsonPathConfig;

    /**
     * Builds a parser that resolves schemas through a freshly created Stencil client.
     *
     * <p>A {@link StencilConfig} is assembled from the sink configuration, the StatsD client and the
     * update listener. When {@link SinkConfig#isSchemaRegistryStencilEnable()} is {@code true} the
     * client is created against the configured Stencil registry URLs; otherwise a default
     * classpath-backed client is used. The JSONPath configuration is initialised with a
     * {@link ProtoJsonProvider} derived from the same sink configuration.</p>
     *
     * @param sinkConfig the sink configuration controlling schema-registry usage and JSON rendering
     * @param reporter the StatsD reporter whose client is supplied to the Stencil configuration for
     *     metrics reporting
     * @param protoUpdateListener listener invoked by Stencil when schemas are refreshed
     */
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

    /**
     * Builds a parser around an already-configured Stencil client and JSONPath configuration.
     *
     * <p>This constructor performs no schema-registry setup of its own and is primarily useful for
     * testing or for reusing a shared client.</p>
     *
     * @param stencilClient the pre-configured Stencil client used for descriptor resolution and
     *     payload decoding
     * @param jsonPathConfig the JSONPath configuration propagated to each parsed message
     */
    public ProtoMessageParser(StencilClient stencilClient, Configuration jsonPathConfig) {
        this.jsonPathConfig = jsonPathConfig;
        this.stencilClient = stencilClient;
    }

    /**
     * Parses the configured portion of a {@link Message} into a {@link ProtoParsedMessage}.
     *
     * <p>The {@code type} selects whether the log key or the log message bytes are parsed. The payload
     * is validated to be a {@code byte[]}, checked for emptiness and then decoded against the given
     * schema class through the Stencil client into a {@link DynamicMessage}, which is finally wrapped
     * in a {@link ProtoParsedMessage}.</p>
     *
     * @param message the inbound message carrying the raw log key and log message bytes
     * @param type whether to parse the {@link SinkConnectorSchemaMessageMode#LOG_KEY} or the
     *     {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} payload
     * @param schemaClass the fully qualified Protobuf class name used to resolve the schema
     * @return a {@link ProtoParsedMessage} wrapping the decoded dynamic message
     * @throws IOException if {@code type} is {@code null}, the payload is not a {@code byte[]}, or the
     *     bytes cannot be decoded against {@code schemaClass}
     * @throws ConfigurationException if {@code type} is not a supported schema mode
     * @throws EmptyMessageException if the selected payload is {@code null} or empty
     */
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

    /**
     * Returns all Protobuf descriptors currently known to the Stencil client.
     *
     * @return a map from registered class or package name to its {@link Descriptors.Descriptor}
     */
    public Map<String, Descriptors.Descriptor> getDescriptorMap() {
        return stencilClient.getAll();
    }

    /**
     * Refreshes the cached schema for the given class by re-fetching its descriptor.
     *
     * <p>Requesting the descriptor causes the Stencil client to reload its cache, which is useful for
     * long-running batch ingestion where the refresh interval exceeds the cache TTL and newer schema
     * versions may have appeared.</p>
     *
     * @param schemaClass the fully qualified Protobuf class name whose descriptor should be refreshed
     */
    @Override
    public void refresh(String schemaClass) {
        // this will try to fetch the descriptor for the class and reloading the cache in the process
        // this is useful for batch ingestion with frequency more than TTL and possibility of new data.
        stencilClient.get(schemaClass);
    }

    /**
     * Builds the alias map from fully qualified Protobuf type name to descriptor registration key.
     *
     * <p>Each descriptor is keyed in the result by its leading-dot full name (for example
     * {@code .com.example.Foo}) mapped to the key under which it is registered in {@code descriptors}.
     * Duplicate full names are removed so that only the first occurrence of each type is retained.</p>
     *
     * @param descriptors the descriptor map to derive aliases from
     * @return a map from leading-dot fully qualified type name to descriptor registration key
     */
    private Map<String, String> getTypeNameToPackageNameMap(Map<String, Descriptors.Descriptor> descriptors) {
        return descriptors.entrySet().stream()
                .filter(distinctByFullName(t -> t.getValue().getFullName()))
                .collect(Collectors.toMap(
                        (mapEntry) -> String.format(".%s", mapEntry.getValue().getFullName()),
                        Map.Entry::getKey));
    }

    /**
     * Creates a stateful predicate that keeps only the first element seen for each derived key.
     *
     * <p>The returned predicate maintains an internal {@link Set} of previously seen keys and returns
     * {@code true} exactly once per distinct key, which enables de-duplication inside a stream
     * pipeline.</p>
     *
     * @param <T> the type of element evaluated by the predicate
     * @param keyExtractor function deriving the distinguishing key from each element
     * @return a predicate that is {@code true} only the first time a given key is encountered
     */
    private <T> Predicate<T> distinctByFullName(Function<? super T, Object> keyExtractor) {
        Set<Object> objects = new HashSet<>();
        return t -> objects.add(keyExtractor.apply(t));
    }

    /**
     * Builds the nested {@link ProtoField} schema tree for a class using the supplied descriptors.
     *
     * @param schemaClass the fully qualified Protobuf class name to describe
     * @param newDescriptors the descriptor map used to resolve the class and its nested types
     * @return the root {@link ProtoField} describing {@code schemaClass} together with its fields
     * @throws IOException if reading the schema fails, as declared by the method contract
     * @throws com.gotocompany.depot.exception.ProtoNotFoundException if no descriptor can be resolved
     *     for {@code schemaClass} or one of its nested types
     */
    public ProtoField getProtoField(String schemaClass, Map<String, Descriptors.Descriptor> newDescriptors) throws IOException {
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, schemaClass, newDescriptors,
                getTypeNameToPackageNameMap(newDescriptors));
        return protoField;
    }

    /**
     * Builds the nested {@link ProtoField} schema tree for a class using the Stencil descriptor map.
     *
     * <p>This is equivalent to {@link #getProtoField(String, Map)} but sources its descriptors from
     * {@link #getDescriptorMap()}.</p>
     *
     * @param schemaClass the fully qualified Protobuf class name to describe
     * @return the root {@link ProtoField} describing {@code schemaClass} together with its fields
     * @throws IOException if reading the schema fails, as declared by the method contract
     * @throws com.gotocompany.depot.exception.ProtoNotFoundException if no descriptor can be resolved
     *     for {@code schemaClass} or one of its nested types
     */
    public ProtoField getProtoField(String schemaClass) throws IOException {
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, schemaClass, getDescriptorMap(),
                getTypeNameToPackageNameMap(getDescriptorMap()));
        return protoField;
    }

    /**
     * Returns the Protobuf descriptor registered for the given schema class.
     *
     * @param schemaClass the fully qualified Protobuf class name to resolve
     * @return the {@link Descriptors.Descriptor} for {@code schemaClass} as provided by the Stencil
     *     client
     */
    public Descriptors.Descriptor getDescriptor(String schemaClass) {
        return stencilClient.get(schemaClass);
    }
}
