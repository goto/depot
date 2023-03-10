package com.gotocompany.depot.bigquery.storage;

import com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoWriter;
import com.gotocompany.depot.bigquery.storage.json.BigQueryJsonWriter;
import com.gotocompany.depot.config.BigQuerySinkConfig;


public class BigQueryWriterFactory {

    public static BigQueryWriter createBigQueryWriter(BigQuerySinkConfig config) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case PROTOBUF:
                return new BigQueryProtoWriter(config);
            case JSON:
                return new BigQueryJsonWriter(config);
            default:
                throw new IllegalArgumentException("Couldn't initialise the BQ writer");
        }
    }
}
