package com.gotocompany.depot.bigquery.storage.proto;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.bigquery.storage.BigQueryStream;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterFactory;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BigQueryProtoWriterTest {

    @Test
    public void shouldInitStreamWriter() {
        BigQuerySinkConfig config = Mockito.mock(BigQuerySinkConfig.class);
        Mockito.when(config.getSinkConnectorSchemaDataType()).thenReturn(SinkConnectorSchemaDataType.PROTOBUF);
        Mockito.when(config.getGCloudProjectID()).thenReturn("test-project");
        Mockito.when(config.getDatasetName()).thenReturn("dataset");
        Mockito.when(config.getTableName()).thenReturn("table");
        BigQueryWriter bigQueryWriter = BigQueryWriterFactory.createBigQueryWriter(config);
        BigQueryWriteClient bqwc = Mockito.mock(BigQueryWriteClient.class);
        CredentialsProvider cp = Mockito.mock(CredentialsProvider.class);
        StreamWriter writer = Mockito.mock(StreamWriter.class);
        BigQueryStream bqs = new BigQueryProtoStream(writer);
        WriteStream ws = Mockito.mock(WriteStream.class);
        TableSchema schema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field1")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("field2")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .build();
        Mockito.when(ws.getTableSchema()).thenReturn(schema);
        Mockito.when(bqwc.getWriteStream(Mockito.any(GetWriteStreamRequest.class))).thenReturn(ws);
        bigQueryWriter.init(c -> bqwc, c -> cp, (c, cr, p) -> bqs);
        Descriptors.Descriptor descriptor = ((BigQueryProtoWriter) bigQueryWriter).getDescriptor();

        Assert.assertEquals(writer, ((BigQueryProtoWriter) bigQueryWriter).streamWriter);

        Assert.assertEquals("field1", descriptor.getFields().get(0).getName());
        Assert.assertEquals(Descriptors.FieldDescriptor.Type.STRING, descriptor.getFields().get(0).getType());
        Assert.assertFalse(descriptor.getFields().get(0).isRepeated());

        Assert.assertEquals("field2", descriptor.getFields().get(1).getName());
        Assert.assertEquals(Descriptors.FieldDescriptor.Type.INT64, descriptor.getFields().get(1).getType());
        Assert.assertTrue(descriptor.getFields().get(1).isRepeated());
    }
}