# HTTP Sink

## Overview
Firehose [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) sink allows users to read data from Kafka and write to an HTTP endpoint. it requires the following [variables](../sinks/http-sink.md#http-sink) to be set. You need to create your own HTTP endpoint so that the Firehose can send data to it.

## Supported methods

Firehose supports `PUT`,`POST`,`PATCH` and `DELETE` verbs in its HTTP sink. The method can be configured using [`SINK_HTTPV2_REQUEST_METHOD`](../sinks/http-sink.md#SINK_HTTPV2_request_method).

## Authentication

Firehose HTTP sink supports [OAuth](https://en.wikipedia.org/wiki/OAuth) authentication. OAuth can be enabled for the HTTP sink by setting [`SINK_HTTPV2_OAUTH2_ENABLE`](../sinks/http-sink.md#SINK_HTTPV2_oauth2_enable)

```text
SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL: https://sample-oauth.my-api.com/oauth2/token  # OAuth2 Token Endpoint.
SINK_HTTPV2_OAUTH2_CLIENT_NAME: client-name  # OAuth2 identifier issued to the client.
SINK_HTTPV2_OAUTH2_CLIENT_SECRET: client-secret # OAuth2 secret issued for the client.
SINK_HTTPV2_OAUTH2_SCOPE: User:read, sys:info  # Space-delimited scope overrides.
```

## Retries

Firehose allows for retrying to sink messages in case of failure of HTTP service. The HTTP error code ranges to retry can be configured with [`SINK_HTTPV2_RETRY_STATUS_CODE_RANGES`](../sinks/http-sink.md#SINK_HTTPV2_retry_status_code_ranges). HTTP request timeout can be configured with [`SINK_HTTPV2_REQUEST_TIMEOUT_MS`](../sinks/http-sink.md#SINK_HTTPV2_request_timeout_ms)


## Templating

Firehose HTTP sink supports payload templating using [`SINK_HTTPV2_JSON_BODY_TEMPLATE`](../sinks/http-sink.md#SINK_HTTPV2_json_body_template) configuration. It uses [JsonPath](https://github.com/json-path/JsonPath) for creating Templates which is a DSL for basic JSON parsing. Playground for this: [https://jsonpath.com/](https://jsonpath.com/), where users can play around with a given JSON to extract out the elements as required and validate the `jsonpath`. The template works only when the output data format [`SINK_HTTPV2_DATA_FORMAT`](../sinks/http-sink.md#SINK_HTTPV2_data_format) is JSON.

###Constants(i.e. without arguments)

Constant values of all data types, i.e. primitive, string, array, object are all supported in the JSON body template.

Examples Templates- 
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=4.5601`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="text"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=true`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE=[23,true,"tdff"]`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE={"err":23,"wee":true}`

Corresponding payloads-
* `4.5601`
* `"text"`
* `true`
* `[23,true,"tdff"]`
* `{"err":23,"wee":true}`


###JSON Primitive data types


All JSON primitive data types are supported, i.e. boolean, integer,long, float. The template will be replaced by the actual data types of the proto, i.e. the parsed template will not be a string. It will be of the type of the Proto field which was passed in the template.

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,float_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,bool_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,int_value"`

Corresponding payloads-
* `4.5601`
* `true`
* `45601`


But if you want the parsed payload to be converted to a string instead of the primitive type then you'll have to follow the below example format -

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",float_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",bool_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",int_value"`

Corresponding payloads-
* `"4.5601"`
* `"true"`
* `"45601"`



If you provide multiple primitive arguments in the template, then the parsed payload will become a string type

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s%s,float_value,int_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s%s,bool_value,int_value"`


Corresponding payloads-
* `"4.560145601"`
* `"true45601"`

###JSON String data type

JSON String data type is supported by providing a string proto field in the template arguments

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s,string_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%s%s,string_value,string_value"`


Corresponding payloads-
* `"dsfweg"`
* `"dsfwegdsfweg"`

Also you can append a constant string to the string proto field template argument

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="sss %saa,string_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="%sa a%s,string_value,string_value"`


Corresponding payloads-
* `"sss dsfwegaa"`
* `"dsfwega adsfweg"`

If you want to convert a primitive/object/array proto field to a string then you'll have to follow the below example format -

Examples Templates-

* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",float_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",bool_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",int_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",message_value"`
* `SINK_HTTPV2_JSON_BODY_TEMPLATE="\"%s\",list_value"`

Corresponding payloads-
* `"4.5601"`
* `"true"`
* `"45601"`
* `"{\"ss\":23,\"ww\":true}"`
* `"[\"wwf\",33,true]"`






