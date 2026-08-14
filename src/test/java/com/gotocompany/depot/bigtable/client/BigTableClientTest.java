package com.gotocompany.depot.bigtable.client;

import com.google.api.gax.rpc.ApiException;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestServiceType;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.bigtable.exception.BigTableInvalidSchemaException;
import com.gotocompany.depot.bigtable.model.BigTableRecord;
import com.gotocompany.depot.bigtable.model.BigTableSchema;
import com.gotocompany.depot.bigtable.response.BigTableResponse;
import com.gotocompany.depot.metrics.BigTableMetrics;
import org.aeonbits.owner.ConfigFactory;
import org.aeonbits.owner.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

/**
 * Unit tests for {@link BigTableClient}, the wrapper around the Bigtable data and admin clients used
 * to apply mutations and validate the destination table.
 *
 * <p>The Google Bigtable clients ({@link BigtableDataClient}, {@link BigtableTableAdminClient}), the
 * {@link BigTableMetrics} and the {@link Instrumentation} are Mockito mocks. The {@link #setUp()}
 * fixture seeds connection and schema system properties, builds two valid {@link BigTableRecord}s and
 * constructs the {@link BigTableClient} under test. Tests cover the success and failure outcomes of
 * {@link BigTableClient#send(java.util.List)} (including a {@link MutateRowsException}), the schema
 * validation paths of {@link BigTableClient#validateBigTableSchema()} that raise a
 * {@link BigTableInvalidSchemaException}, and the latency and count metrics emitted on a successful
 * bulk mutation.</p>
 */
public class BigTableClientTest {

    /** Mock Bigtable data client whose bulk-mutation behaviour is stubbed per test. */
    @Mock
    private BigtableDataClient bigTableDataClient;
    /** Mock Bigtable admin client used to stub table existence and lookups. */
    @Mock
    private BigtableTableAdminClient bigtableTableAdminClient;
    /** Mock API exception used to populate failed mutations. */
    @Mock
    private ApiException apiException;
    /** Mock metrics supplying metric names and tags used in verifications. */
    @Mock
    private BigTableMetrics bigtableMetrics;
    /** Mock instrumentation whose logging and metric calls are verified. */
    @Mock
    private Instrumentation instrumentation;

    /** Client under test, built from the mocked clients and schema. */
    private BigTableClient bigTableClient;
    /** Two valid records sent through the client. */
    private List<BigTableRecord> validRecords;
    /** Sink configuration loaded from the seeded system properties. */
    private BigTableSinkConfig sinkConfig;

    /**
     * Seeds connection and schema configuration and builds the client under test before each test.
     *
     * <p>Opens the Mockito annotations, sets the proto class, message mode, GCP project, instance,
     * table, credential path, column-family mapping and row-key template as system properties, builds
     * two valid {@link BigTableRecord}s and a {@link BigTableSchema}, and constructs the
     * {@link BigTableClient} from the mocked data client, admin client, metrics and
     * instrumentation.</p>
     *
     * @throws IOException if loading configuration fails
     */
    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestBookingLogMessage");
        System.setProperty("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        System.setProperty("SINK_BIGTABLE_GOOGLE_CLOUD_PROJECT_ID", "test-gcloud-project");
        System.setProperty("SINK_BIGTABLE_INSTANCE_ID", "test-instance");
        System.setProperty("SINK_BIGTABLE_TABLE_ID", "test-table");
        System.setProperty("SINK_BIGTABLE_CREDENTIAL_PATH", "Users/github/bigtable/test-credential");
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"family-test\" : { \"qualifier_name1\" : \"input_field1\", \"qualifier_name2\" : \"input_field2\"} }");
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key-constant-string");

        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").setServiceType(TestServiceType.Enum.GO_SEND).build();
        TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").build();
        TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").setServiceType(TestServiceType.Enum.GO_SHOP).build();

        Message message1 = new Message(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        Message message2 = new Message(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());

        RowMutationEntry rowMutationEntry = RowMutationEntry.create("rowKey").setCell("family", "qualifier", "value");
        BigTableRecord bigTableRecord1 = new BigTableRecord(rowMutationEntry, 1, null, message1.getMetadata());
        BigTableRecord bigTableRecord2 = new BigTableRecord(rowMutationEntry, 2, null, message2.getMetadata());
        validRecords = Collections.list(bigTableRecord1, bigTableRecord2);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        BigTableSchema schema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableClient = new BigTableClient(sinkConfig, bigTableDataClient, bigtableTableAdminClient, schema, bigtableMetrics, instrumentation);
    }

    /**
     * Verifies that a successful bulk mutation yields a null response (no errors).
     *
     * <p>Given the data client's {@code bulkMutateRows} stubbed to do nothing, when
     * {@link BigTableClient#send(java.util.List)} is called with valid records, then the returned
     * {@link BigTableResponse} is {@code null}.</p>
     */
    @Test
    public void shouldReturnNullBigTableResponseWhenBulkMutateRowsDoesNotThrowAnException() {
        doNothing().when(bigTableDataClient).bulkMutateRows(isA(BulkMutation.class));

        BigTableResponse bigTableResponse = bigTableClient.send(validRecords);

        Assert.assertNull(bigTableResponse);
    }

    /**
     * Verifies that a {@link MutateRowsException} is surfaced as a response carrying failed mutations.
     *
     * <p>Given {@code bulkMutateRows} stubbed to throw a {@link MutateRowsException} with two failed
     * mutations, when records are sent, then the returned {@link BigTableResponse} reports errors with
     * two failed mutations and the failure cause is logged once through the {@link Instrumentation}.</p>
     */
    @Test
    public void shouldReturnBigTableResponseWithFailedMutationsWhenBulkMutateRowsThrowsMutateRowsException() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(0, apiException));
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);

        doThrow(mutateRowsException).when(bigTableDataClient).bulkMutateRows(isA(BulkMutation.class));

        BigTableResponse bigTableResponse = bigTableClient.send(validRecords);

        Assert.assertTrue(bigTableResponse.hasErrors());
        Assert.assertEquals(2, bigTableResponse.getFailedMutations().size());
        Mockito.verify(instrumentation, Mockito.times(1)).logError("Some entries failed to be applied. {}", mutateRowsException.getCause());
    }

    /**
     * Verifies that validating against a missing table reports the full table path.
     *
     * <p>Given the admin client stubbed so the configured table does not exist, when
     * {@link BigTableClient#validateBigTableSchema()} is invoked, then a
     * {@link BigTableInvalidSchemaException} is thrown whose message gives the
     * {@code projects/.../instances/.../tables/...} path of the missing table.</p>
     */
    @Test
    public void shouldThrowInvalidSchemaExceptionIfTableDoesNotExist() {
        when(bigtableTableAdminClient.exists(sinkConfig.getTableId())).thenReturn(false);
        when(bigtableTableAdminClient.getProjectId()).thenReturn(sinkConfig.getGCloudProjectID());
        when(bigtableTableAdminClient.getInstanceId()).thenReturn(sinkConfig.getInstanceId());
        try {
            bigTableClient.validateBigTableSchema();
        } catch (BigTableInvalidSchemaException e) {
            Assert.assertEquals("Table not found on the path: projects/test-gcloud-project/instances/test-instance/tables/test-table", e.getMessage());
        }
    }

    /**
     * Verifies that validating against a table missing a configured column family is rejected.
     *
     * <p>Given an existing table that declares only {@code existing-family-test}, when
     * {@link BigTableClient#validateBigTableSchema()} is invoked, then a
     * {@link BigTableInvalidSchemaException} is thrown reporting that the configured
     * {@code family-test} family does not exist in the table.</p>
     */
    @Test
    public void shouldThrowInvalidSchemaExceptionIfColumnFamilyDoesNotExist() {
        Table testTable = Table.fromProto(com.google.bigtable.admin.v2.Table.newBuilder()
                .setName("projects/" + sinkConfig.getGCloudProjectID() + "/instances/" + sinkConfig.getInstanceId() + "/tables/" + sinkConfig.getTableId())
                .putColumnFamilies("existing-family-test", ColumnFamily.newBuilder().build())
                .build());

        when(bigtableTableAdminClient.exists(sinkConfig.getTableId())).thenReturn(true);
        when(bigtableTableAdminClient.getTable(sinkConfig.getTableId())).thenReturn(testTable);
        try {
            bigTableClient.validateBigTableSchema();
        } catch (BigTableInvalidSchemaException e) {
            Assert.assertEquals("Column families [family-test] do not exist in table test-table!", e.getMessage());
        }
    }

    /**
     * Verifies that a successful bulk mutation emits the operation latency and count metrics.
     *
     * <p>Given {@code bulkMutateRows} stubbed to do nothing, when records are sent, then the
     * {@link Instrumentation} records the Bigtable operation latency once and the operation total
     * count once, each tagged with the configured instance and table.</p>
     */
    @Test
    public void shouldCaptureBigtableMetricsWhenBulkMutateRowsDoesNotThrowAnException() {
        doNothing().when(bigTableDataClient).bulkMutateRows(isA(BulkMutation.class));

        bigTableClient.send(validRecords);

        Mockito.verify(instrumentation, Mockito.times(1)).captureDurationSince(eq(bigtableMetrics.getBigtableOperationLatencyMetric()),
                any(),
                eq(String.format(BigTableMetrics.BIGTABLE_INSTANCE_TAG, sinkConfig.getInstanceId())),
                eq(String.format(BigTableMetrics.BIGTABLE_TABLE_TAG, sinkConfig.getTableId())));
        Mockito.verify(instrumentation, Mockito.times(1)).captureCount(eq(bigtableMetrics.getBigtableOperationTotalMetric()),
                any(),
                eq(String.format(BigTableMetrics.BIGTABLE_INSTANCE_TAG, sinkConfig.getInstanceId())),
                eq(String.format(BigTableMetrics.BIGTABLE_TABLE_TAG, sinkConfig.getTableId())));
    }
}
