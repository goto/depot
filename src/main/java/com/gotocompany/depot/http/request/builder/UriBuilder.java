package com.gotocompany.depot.http.request.builder;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Builds the target {@link URI} of an HTTP request from a (possibly templated) service URL and query
 * parameters.
 *
 * <p>The configured service URL is compiled into a {@link Template} so it may embed per-message
 * placeholders. In single mode the URL is resolved against a message's parsed content (key or value,
 * per the configured schema message mode) before query parameters are appended. In batch mode the URL
 * must be a constant string; a templated URL is rejected because one request serves many messages.</p>
 *
 * @see HeaderBuilder
 * @see QueryParamBuilder
 * @see Template
 */
public class UriBuilder {

    /**
     * Compiled template of the configured service URL; may be constant or contain placeholders.
     */
    private final Template urlTemplate;
    /**
     * Schema message mode determining whether the URL template resolves against the key or the value.
     */
    private final SinkConnectorSchemaMessageMode sourceType;
    /**
     * Fully qualified schema (Protobuf) class name used when parsing the message key.
     */
    private final String schemaProtoKeyClass;
    /**
     * Fully qualified schema (Protobuf) class name used when parsing the message value.
     */
    private final String schemaProtoMessageClass;

    /**
     * Creates a URI builder by compiling the configured service URL into a template.
     *
     * @param config the HTTP sink configuration supplying the service URL, schema message mode and
     *     schema classes
     * @throws InvalidTemplateException if the configured service URL is not a valid template
     */
    public UriBuilder(HttpSinkConfig config) throws InvalidTemplateException {
        this.urlTemplate = new Template(config.getSinkHttpServiceUrl());
        this.sourceType = config.getSinkConnectorSchemaMessageMode();
        this.schemaProtoKeyClass = config.getSinkConnectorSchemaProtoKeyClass();
        this.schemaProtoMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
    }

    /**
     * Builds the request URI for a single message by resolving the URL template and appending query
     * parameters.
     *
     * <p>The URL template is resolved against the parsed key in
     * {@link SinkConnectorSchemaMessageMode#LOG_KEY} mode, or the parsed value otherwise, and the
     * supplied query parameters are then appended.</p>
     *
     * @param container the lazily-parsed view of the message
     * @param queryParam the query parameters to append to the resolved URL
     * @return the fully resolved request {@link URI}
     * @throws IOException if the required message portion cannot be parsed
     * @throws ConfigurationException if the resolved URL is syntactically invalid
     */
    public URI build(MessageContainer container, Map<String, String> queryParam) throws IOException {
        if (sourceType == SinkConnectorSchemaMessageMode.LOG_KEY) {
            return build(urlTemplate.parse(
                    container.getParsedLogKey(schemaProtoKeyClass)),
                    queryParam);
        } else {
            return build(urlTemplate.parse(
                    container.getParsedLogMessage(schemaProtoMessageClass)),
                    queryParam);
        }
    }

    /**
     * Builds the request URI for batch mode from the constant service URL and query parameters.
     *
     * <p>Because a batch request serves many messages, a templated service URL is not permitted; the
     * URL must be a constant string or a {@link ConfigurationException} is thrown.</p>
     *
     * @param queryParam the constant query parameters to append to the URL
     * @return the request {@link URI} for the batch
     * @throws ConfigurationException if the configured service URL contains template variables, or if
     *     the URL is syntactically invalid
     */
    public URI build(Map<String, String> queryParam) {
        if (!urlTemplate.isConstantString()) {
            throw new ConfigurationException("Template in Service URL is not allowed in batch request mode.");
        }
        return build(urlTemplate.getTemplateString(), queryParam);
    }

    /**
     * Constructs a {@link URI} from a resolved URL string and a set of query parameters.
     *
     * <p>Each query parameter is added to an {@link org.apache.http.client.utils.URIBuilder} seeded
     * with {@code url}, and the built URI is returned.</p>
     *
     * @param url the fully resolved URL string
     * @param queryParam the query parameters to append
     * @return the constructed {@link URI}
     * @throws ConfigurationException if {@code url} is not a syntactically valid URI
     */
    private URI build(String url, Map<String, String> queryParam) {
        try {
            URIBuilder uriBuilder = new URIBuilder(url);
            queryParam.forEach(uriBuilder::addParameter);
            return uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new ConfigurationException("Service URL '" + url + "' is invalid");
        }
    }
}
