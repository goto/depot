package com.gotocompany.depot.bigquery.storage;

import com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStorageClient;
import com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoWriter;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.MessageParser;

public class BigQueryPayloadConvertorFactory {
    public static BigQueryStorageClient createBigQueryPayloadConvertor(
            BigQuerySinkConfig config,
            MessageParser parser,
            BigQueryProtoWriter bigQueryWriter) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case PROTOBUF:
                return new BigQueryProtoStorageClient(bigQueryWriter, config, parser);
            default:
                throw new IllegalArgumentException("Invalid data type");
        }
    }
}
