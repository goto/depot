package com.gotocompany.depot.http.request.builder;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.common.TemplateUtils;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.http.enums.HttpParameterSourceType;
import com.gotocompany.depot.message.MessageContainer;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Builds the query parameters appended to the request URL, supporting static and templated values.
 *
 * <p>Query parameters are described as a map of templates for both key and value. In single mode each
 * template is resolved against a message's parsed content (key or value, per the configured
 * {@link HttpParameterSourceType}). In batch mode, where one request serves many messages, only
 * constant query templates are permitted and any template containing variables is rejected.</p>
 *
 * @see HeaderBuilder
 * @see UriBuilder
 * @see HttpParameterSourceType
 */
public class QueryParamBuilder {

    /**
     * Query parameter templates (key and value) resolved per message or required to be constant in
     * batch mode.
     */
    private final Map<Template, Template> queryParamTemplates;
    /**
     * Whether query templates are resolved from the parsed key or the parsed value of a message.
     */
    private final HttpParameterSourceType queryParameterSource;
    /**
     * Fully qualified schema (Protobuf) class name used when parsing the message key.
     */
    private final String schemaProtoKeyClass;
    /**
     * Fully qualified schema (Protobuf) class name used when parsing the message value.
     */
    private final String schemaProtoMessageClass;


    /**
     * Creates a query parameter builder from the sink configuration.
     *
     * @param config the HTTP sink configuration supplying the query templates, the query parameter
     *     source and the schema classes
     */
    public QueryParamBuilder(HttpSinkConfig config) {
        this.queryParamTemplates = config.getQueryTemplate();
        this.queryParameterSource = config.getQueryParamSourceMode();
        this.schemaProtoKeyClass = config.getSinkConnectorSchemaProtoKeyClass();
        this.schemaProtoMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
    }

    /**
     * Returns the constant query parameters for batch mode, rejecting any templated entries.
     *
     * <p>Each template key and value must be a constant string; if any contains variables a
     * {@link ConfigurationException} is thrown, since per-message query parameters cannot be used when
     * a single request serves the whole batch. The constant templates are collected into a plain
     * name-to-value map.</p>
     *
     * @return a map of constant query parameter names to values
     * @throws ConfigurationException if any query template key or value is not a constant string
     */
    public Map<String, String> build() {

        return queryParamTemplates
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        templateKey -> {
                            if (!templateKey.getKey().isConstantString()) {
                                throw new ConfigurationException("Params in query template are not allowed in batch request mode.");
                            }
                            return templateKey.getKey().getTemplateString();
                        },
                        templateValue -> {
                            if (!templateValue.getValue().isConstantString()) {
                                throw new ConfigurationException("Params in query template are not allowed in batch request mode.");
                            }
                            return templateValue.getValue().getTemplateString();
                        }
                ));
    }

    /**
     * Builds the query parameters for a single message by resolving the configured templates.
     *
     * <p>The templates are evaluated against either the parsed key or the parsed value of the message,
     * depending on the configured {@link HttpParameterSourceType}, producing a name-to-value map.</p>
     *
     * @param msgContainer the lazily-parsed view of the message
     * @return a map of resolved query parameter names to values
     * @throws IOException if the required message portion cannot be parsed
     */
    public Map<String, String> build(MessageContainer msgContainer) throws IOException {
        if (queryParameterSource == HttpParameterSourceType.KEY) {
            return TemplateUtils.parseTemplateMap(
                    queryParamTemplates,
                    msgContainer.getParsedLogKey(schemaProtoKeyClass)
            );
        } else {
            return TemplateUtils.parseTemplateMap(
                    queryParamTemplates,
                    msgContainer.getParsedLogMessage(schemaProtoMessageClass)
            );
        }
    }
}
