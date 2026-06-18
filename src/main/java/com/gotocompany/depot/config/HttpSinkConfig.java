package com.gotocompany.depot.config;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.config.converter.HttpHeaderConverter;
import com.gotocompany.depot.config.converter.HttpParameterSourceTypeConverter;
import com.gotocompany.depot.config.converter.HttpRequestBodyTypeConverter;
import com.gotocompany.depot.config.converter.HttpRequestMethodConverter;
import com.gotocompany.depot.config.converter.HttpRequestTypeConverter;
import com.gotocompany.depot.config.converter.RangeToHashMapConverter;
import com.gotocompany.depot.config.converter.TemplateMapConverter;
import com.gotocompany.depot.http.enums.HttpParameterSourceType;
import com.gotocompany.depot.http.enums.HttpRequestBodyType;
import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import com.gotocompany.depot.http.enums.HttpRequestType;
import org.aeonbits.owner.Config;

import java.util.Map;


/**
 * Owner configuration interface for Depot's HTTP sink request shaping.
 *
 * <p>{@code HttpSinkConfig} builds on {@link HttpClientConfig} (and transitively {@link SinkConfig})
 * by describing how each outbound HTTP request is constructed: the service URL and method, static and
 * templated headers, query parameters, the request and body modes (single versus batch, and raw,
 * JSON, message, or templatized JSON bodies), the status-code ranges that are logged or retried, and
 * delete-body behaviour. As with the other HTTP settings, all properties use the {@code SINK_HTTPV2_*}
 * namespace. The {@code @Config.DisableFeature(PARAMETER_FORMATTING)} annotation disables Owner's
 * parameter expansion so that values are used literally.
 */
@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface HttpSinkConfig extends HttpClientConfig {

    /**
     * Returns the destination URL to which HTTP requests are sent.
     *
     * <p>In single-request mode the URL may embed templates that are resolved per message; in batch
     * mode templating in the URL is not permitted. Bound to the {@code SINK_HTTPV2_SERVICE_URL}
     * property; has no default.
     *
     * @return the HTTP service URL
     */
    @Key("SINK_HTTPV2_SERVICE_URL")
    String getSinkHttpServiceUrl();

    /**
     * Returns the HTTP method used for outbound requests.
     *
     * <p>Bound to the {@code SINK_HTTPV2_REQUEST_METHOD} property, converted with
     * {@link HttpRequestMethodConverter}, and defaulting to {@code put}. Supported values are defined
     * by {@link HttpRequestMethodType}, namely {@code PUT}, {@code POST}, {@code PATCH}, and
     * {@code DELETE}.
     *
     * @return the configured HTTP request method
     */
    @Key("SINK_HTTPV2_REQUEST_METHOD")
    @DefaultValue("put")
    @ConverterClass(HttpRequestMethodConverter.class)
    HttpRequestMethodType getSinkHttpRequestMethod();

    /**
     * Returns the static HTTP headers attached to every request.
     *
     * <p>Bound to the {@code SINK_HTTPV2_HEADERS} property and parsed by {@link HttpHeaderConverter}
     * from a comma-separated list of {@code name:value} pairs into a {@code Map<String, String>}; it
     * defaults to an empty string, yielding no static headers.
     *
     * @return the static request headers keyed by header name
     */
    @Key("SINK_HTTPV2_HEADERS")
    @DefaultValue("")
    @ConverterClass(HttpHeaderConverter.class)
    Map<String, String> getSinkHttpHeaders();

    /**
     * Returns the templated HTTP headers whose names and values are resolved per message.
     *
     * <p>Bound to the {@code SINK_HTTPV2_HEADERS_TEMPLATE} property and parsed by
     * {@link TemplateMapConverter} from a JSON object into a {@code Map<Template, Template>}; it
     * defaults to an empty string, yielding no templated headers. Each key and value is a
     * {@link Template} evaluated against the source selected by
     * {@link #getSinkHttpHeadersParameterSource()}.
     *
     * @return the templated request headers as a map of header-name to header-value templates
     */
    @Key("SINK_HTTPV2_HEADERS_TEMPLATE")
    @DefaultValue("")
    @ConverterClass(TemplateMapConverter.class)
    Map<Template, Template> getSinkHttpHeadersTemplate();

    /**
     * Returns the data source used to resolve templated header values.
     *
     * <p>Bound to the {@code SINK_HTTPV2_HEADERS_PARAMETER_SOURCE} property, converted with
     * {@link HttpParameterSourceTypeConverter}, and defaulting to
     * {@link HttpParameterSourceType#MESSAGE}. It selects whether header templates read from the
     * message body ({@link HttpParameterSourceType#MESSAGE}) or the record key
     * ({@link HttpParameterSourceType#KEY}).
     *
     * @return the parameter source for header templates
     */
    @Key("SINK_HTTPV2_HEADERS_PARAMETER_SOURCE")
    @DefaultValue("MESSAGE")
    @ConverterClass(HttpParameterSourceTypeConverter.class)
    HttpParameterSourceType getSinkHttpHeadersParameterSource();

    /**
     * Returns the templated query parameters appended to the request URL.
     *
     * <p>Bound to the {@code SINK_HTTPV2_QUERY_TEMPLATE} property and parsed by
     * {@link TemplateMapConverter} from a JSON object into a {@code Map<Template, Template>}; it
     * defaults to an empty string, yielding no query parameters, and is not permitted in batch mode.
     * Each key and value is a {@link Template} evaluated against the source selected by
     * {@link #getQueryParamSourceMode()}.
     *
     * @return the templated query parameters as a map of name to value templates
     */
    @Key("SINK_HTTPV2_QUERY_TEMPLATE")
    @DefaultValue("")
    @ConverterClass(TemplateMapConverter.class)
    Map<Template, Template> getQueryTemplate();

    /**
     * Returns the data source used to resolve templated query parameters.
     *
     * <p>Bound to the {@code SINK_HTTPV2_QUERY_PARAMETER_SOURCE} property, converted with
     * {@link HttpParameterSourceTypeConverter}, and defaulting to
     * {@link HttpParameterSourceType#MESSAGE}. It selects whether query templates read from the
     * message body ({@link HttpParameterSourceType#MESSAGE}) or the record key
     * ({@link HttpParameterSourceType#KEY}).
     *
     * @return the parameter source for query templates
     */
    @Key("SINK_HTTPV2_QUERY_PARAMETER_SOURCE")
    @DefaultValue("MESSAGE")
    @ConverterClass(HttpParameterSourceTypeConverter.class)
    HttpParameterSourceType getQueryParamSourceMode();

    /**
     * Returns the request batching mode of the HTTP sink.
     *
     * <p>Bound to the {@code SINK_HTTPV2_REQUEST_MODE} property, converted with
     * {@link HttpRequestTypeConverter}, and defaulting to {@link HttpRequestType#SINGLE}. In
     * {@link HttpRequestType#SINGLE} mode one request is issued per record, whereas
     * {@link HttpRequestType#BATCH} groups multiple records into a single request (which disallows
     * templating in the URL, headers, and query parameters).
     *
     * @return the configured HTTP request mode
     */
    @Key("SINK_HTTPV2_REQUEST_MODE")
    @DefaultValue("SINGLE")
    @ConverterClass(HttpRequestTypeConverter.class)
    HttpRequestType getRequestType();

    /**
     * Returns the way the request body is constructed from each record.
     *
     * <p>Bound to the {@code SINK_HTTPV2_REQUEST_BODY_MODE} property, converted with
     * {@link HttpRequestBodyTypeConverter}, and defaulting to {@link HttpRequestBodyType#RAW}. The
     * value selects among {@link HttpRequestBodyType#RAW}, {@link HttpRequestBodyType#JSON},
     * {@link HttpRequestBodyType#MESSAGE}, and {@link HttpRequestBodyType#TEMPLATIZED_JSON}, the last
     * of which uses {@link #getSinkHttpJsonBodyTemplate()}.
     *
     * @return the configured request body mode
     */
    @Key("SINK_HTTPV2_REQUEST_BODY_MODE")
    @DefaultValue("RAW")
    @ConverterClass(HttpRequestBodyTypeConverter.class)
    HttpRequestBodyType getRequestBodyType();

    /**
     * Returns the set of HTTP response status codes whose responses should be logged.
     *
     * <p>Bound to the {@code SINK_HTTPV2_REQUEST_LOG_STATUS_CODE_RANGES} property and expanded by
     * {@link RangeToHashMapConverter} from one or more {@code start-end} ranges into a
     * {@code Map<Integer, Boolean>} that maps each in-range status code to {@code true}; it defaults to
     * {@code 400-600}.
     *
     * @return a map whose keys are the status codes selected for logging
     */
    @Key("SINK_HTTPV2_REQUEST_LOG_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRequestLogStatusCodeRanges();

    /**
     * Returns the set of HTTP response status codes that should trigger a retry.
     *
     * <p>Bound to the {@code SINK_HTTPV2_RETRY_STATUS_CODE_RANGES} property and expanded by
     * {@link RangeToHashMapConverter} from one or more {@code start-end} ranges into a
     * {@code Map<Integer, Boolean>} that maps each in-range status code to {@code true}; it defaults to
     * {@code 400-600}.
     *
     * @return a map whose keys are the status codes selected for retry
     */
    @Key("SINK_HTTPV2_RETRY_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRetryStatusCodeRanges();

    /**
     * Returns the JSON template used to build the request body in templatized-JSON mode.
     *
     * <p>Used when {@link #getRequestBodyType()} is {@link HttpRequestBodyType#TEMPLATIZED_JSON}; the
     * template is rendered against each message to produce the JSON payload. Bound to the
     * {@code SINK_HTTPV2_JSON_BODY_TEMPLATE} property; defaults to an empty string.
     *
     * @return the JSON body template, or an empty string when not configured
     */
    @Key("SINK_HTTPV2_JSON_BODY_TEMPLATE")
    @DefaultValue("")
    String getSinkHttpJsonBodyTemplate();

    /**
     * Indicates whether Protobuf default values are emitted for fields absent from a message.
     *
     * <p>This HTTP-specific property is read from the {@code SINK_HTTPV2_DEFAULT_FIELD_VALUE_ENABLE}
     * key and shadows {@link SinkConfig#getSinkDefaultFieldValueEnable()}. When {@code true}, fields
     * not present in the payload are serialized with their Protobuf default value; when {@code false},
     * such fields are omitted. Defaults to {@code true}.
     *
     * @return {@code true} if default field values should be written, {@code false} otherwise
     */
    @Key("SINK_HTTPV2_DEFAULT_FIELD_VALUE_ENABLE")
    @DefaultValue("true")
    boolean getSinkDefaultFieldValueEnable();

    /**
     * Indicates whether a request body is sent with HTTP {@code DELETE} requests.
     *
     * <p>When {@code true}, a body is included on delete requests; when {@code false}, delete requests
     * are sent without a body. Bound to the {@code SINK_HTTPV2_DELETE_BODY_ENABLE} property; defaults
     * to {@code true}.
     *
     * @return {@code true} if a body should be sent with delete requests, {@code false} otherwise
     */
    @Key("SINK_HTTPV2_DELETE_BODY_ENABLE")
    @DefaultValue("true")
    Boolean isSinkHttpDeleteBodyEnable();
}
