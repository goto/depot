package com.gotocompany.depot.http.request.builder;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.common.TemplateUtils;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.http.enums.HttpParameterSourceType;
import com.gotocompany.depot.message.MessageContainer;
import lombok.Getter;

import java.io.IOException;
import java.util.Map;

/**
 * Builds the HTTP request headers for the sink, supporting both static and per-message templated
 * headers.
 *
 * <p>Headers come from two sources: a map of constant base headers and a map of header templates whose
 * keys and values are resolved against a message's parsed content. In batch mode only the constant
 * base headers are allowed, whereas in single mode the templated headers are evaluated per message and
 * merged with the base headers. Which message portion the templates are resolved from (parsed key or
 * parsed value) is controlled by the configured {@link HttpParameterSourceType}.</p>
 *
 * <p>The Lombok {@code @Getter} annotation exposes accessors for all fields.</p>
 *
 * @see QueryParamBuilder
 * @see UriBuilder
 * @see HttpParameterSourceType
 */
@Getter
public class HeaderBuilder {

    /**
     * Constant headers applied to every request, independent of message content.
     */
    private final Map<String, String> baseHeaders;
    /**
     * Header templates (key and value) resolved per message; empty when none are configured.
     */
    private final Map<Template, Template> headersTemplate;
    /**
     * Whether header templates are resolved from the parsed key or the parsed value of a message.
     */
    private final HttpParameterSourceType headersParameterSource;
    /**
     * Fully qualified schema (Protobuf) class name used when parsing the message key.
     */
    private final String schemaProtoKeyClass;
    /**
     * Fully qualified schema (Protobuf) class name used when parsing the message value.
     */
    private final String schemaProtoMessageClass;

    /**
     * Creates a header builder from the sink configuration.
     *
     * @param config the HTTP sink configuration supplying base headers, header templates, the header
     *     parameter source and the schema classes
     */
    public HeaderBuilder(HttpSinkConfig config) {
        this.baseHeaders = config.getSinkHttpHeaders();
        this.headersTemplate = config.getSinkHttpHeadersTemplate();
        this.headersParameterSource = config.getSinkHttpHeadersParameterSource();
        this.schemaProtoKeyClass = config.getSinkConnectorSchemaProtoKeyClass();
        this.schemaProtoMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
    }

    /**
     * Returns the constant headers for batch mode, rejecting any configured header templates.
     *
     * <p>Because a batch request serves many messages, per-message header templates are not
     * permitted; if any are configured this method fails fast.</p>
     *
     * @return the constant base headers
     * @throws ConfigurationException if any header templates are configured
     */
    public Map<String, String> build() {
        if (!headersTemplate.isEmpty()) {
            throw new ConfigurationException("Header template is not allowed in batch request mode.");
        }
        return baseHeaders;
    }

    /**
     * Builds the headers for a single message by resolving the header templates and merging them with
     * the base headers.
     *
     * <p>The header templates are evaluated against either the parsed key or the parsed value of the
     * message, depending on the configured {@link HttpParameterSourceType}, and the resulting entries
     * are merged into the base headers. The combined map is then returned.</p>
     *
     * @param msgContainer the lazily-parsed view of the message
     * @return the merged map of base headers and resolved templated headers
     * @throws IOException if the required message portion cannot be parsed
     */
    public Map<String, String> build(MessageContainer msgContainer) throws IOException {
        Map<String, String> headers;
        if (headersParameterSource == HttpParameterSourceType.KEY) {
            headers = TemplateUtils.parseTemplateMap(
                    headersTemplate,
                    msgContainer.getParsedLogKey(schemaProtoKeyClass)
            );
        } else {
            headers = TemplateUtils.parseTemplateMap(
                    headersTemplate,
                    msgContainer.getParsedLogMessage(schemaProtoMessageClass)
            );
        }
        baseHeaders.putAll(headers);
        return baseHeaders;
    }
}
