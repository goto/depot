package com.gotocompany.depot.bigquery.storage.proto;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStream;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterFactory;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BigQueryProtoWriterTest {
    private BigQueryWriter bigQueryWriter;
    private StreamWriter writer;

    @Before
    public void setup() {
        BigQuerySinkConfig config = Mockito.mock(BigQuerySinkConfig.class);
        Mockito.when(config.getSinkConnectorSchemaDataType()).thenReturn(SinkConnectorSchemaDataType.PROTOBUF);
        Mockito.when(config.getGCloudProjectID()).thenReturn("test-project");
        Mockito.when(config.getDatasetName()).thenReturn("dataset");
        Mockito.when(config.getTableName()).thenReturn("table");
        BigQueryWriteClient bqwc = Mockito.mock(BigQueryWriteClient.class);
        CredentialsProvider cp = Mockito.mock(CredentialsProvider.class);
        writer = Mockito.mock(StreamWriter.class);
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
        bigQueryWriter = BigQueryWriterFactory.createBigQueryWriter(config, c -> bqwc, c -> cp, (c, cr, p) -> bqs);
        bigQueryWriter.init();
    }

    @Test
    public void shouldInitStreamWriter() {
        Descriptors.Descriptor descriptor = ((BigQueryProtoWriter) bigQueryWriter).getDescriptor();
        Assert.assertEquals(writer, ((BigQueryProtoWriter) bigQueryWriter).getStreamWriter());
        Assert.assertEquals("field1", descriptor.getFields().get(0).getName());
        Assert.assertEquals(Descriptors.FieldDescriptor.Type.STRING, descriptor.getFields().get(0).getType());
        Assert.assertFalse(descriptor.getFields().get(0).isRepeated());
        Assert.assertEquals("field2", descriptor.getFields().get(1).getName());
        Assert.assertEquals(Descriptors.FieldDescriptor.Type.INT64, descriptor.getFields().get(1).getType());
        Assert.assertTrue(descriptor.getFields().get(1).isRepeated());
    }

    @Test
    public void shouldAppendAndGet() throws Exception {
        ProtoRows rows = Mockito.mock(ProtoRows.class);
        com.gotocompany.depot.bigquery.storage.BigQueryPayload payload = new BigQueryPayload();
        payload.setPayload(rows);
        ApiFuture<AppendRowsResponse> future = Mockito.mock(ApiFuture.class);
        AppendRowsResponse apiResponse = Mockito.mock(AppendRowsResponse.class);
        Mockito.when(future.get()).thenReturn(apiResponse);
        Mockito.when(writer.append(rows)).thenReturn(future);
        AppendRowsResponse appendRowsResponse = bigQueryWriter.appendAndGet(payload);
        Assert.assertEquals(apiResponse, appendRowsResponse);
    }
}
