package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.client.MaxComputeClient;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.exception.MaxComputeTableOperationException;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MaxComputeSchemaCache}.
 *
 * <p>These tests verify how the cache lazily builds, caches, and refreshes the MaxCompute schema in response to
 * Stencil descriptor updates. The {@link MaxComputeSchemaBuilder} is mocked to return a placeholder
 * {@link MaxComputeSchema}, the {@link MaxComputeClient} is a Mockito spy whose table create/update and
 * latest-schema reads are stubbed, and {@link SinkConfig} selects between {@code LOG_MESSAGE} and
 * {@code LOG_KEY} schema modes. Interactions are asserted by verifying how often the client upserts the table
 * and, in one case, by injecting the cached schema through reflection.</p>
 *
 * @see MaxComputeSchemaCache
 */
public class MaxComputeSchemaCacheTest {

    /**
     * Verifies that the schema is lazily built, the table is upserted, and the server-side schema is cached on
     * first access.
     *
     * <p>Given a freshly constructed cache whose collaborators are stubbed, when
     * {@link MaxComputeSchemaCache#getMaxComputeSchema()} is called for the first time, then the table is
     * created or updated exactly once and the returned schema's table schema equals the server-side
     * {@link TableSchema} read back from the {@link MaxComputeClient}.</p>
     *
     * @throws OdpsException never in this test; declared because the upsert path may throw it
     */
    @Test
    public void shouldBuildAndReturnMaxComputeSchema() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = Mockito.mock(MaxComputeSchemaBuilder.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        when(protoMessageParser.getDescriptorMap()).thenReturn(newDescriptor);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaBuilder.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        when(sinkConfig.getSinkConnectorSchemaProtoKeyClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaBuilder,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doNothing()
                .when(maxComputeClient)
                .createOrUpdateTable(Mockito.any());
        TableSchema finalMockedTableSchema = Mockito.mock(TableSchema.class);
        Mockito.doReturn(finalMockedTableSchema)
                .when(maxComputeClient)
                .getLatestTableSchema();

        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();

        verify(maxComputeClient, Mockito.times(1))
                .createOrUpdateTable(Mockito.any());
        assertEquals(finalMockedTableSchema, maxComputeSchema.getTableSchema());
    }

    /**
     * Verifies that an already-cached schema is returned without rebuilding the table.
     *
     * <p>Given a cache whose private {@code maxComputeSchema} field is pre-populated via reflection, when
     * {@link MaxComputeSchemaCache#getMaxComputeSchema()} is called, then the cached instance is returned and
     * the {@link MaxComputeClient} table upsert is never invoked.</p>
     *
     * @throws OdpsException           never in this test; declared because the upsert path may throw it
     * @throws NoSuchFieldException    if the reflective {@code maxComputeSchema} field lookup fails
     * @throws IllegalAccessException  if the reflective field assignment is not permitted
     */
    @Test
    public void shouldReturnMaxComputeSchemaIfExists() throws OdpsException, NoSuchFieldException, IllegalAccessException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = Mockito.mock(MaxComputeSchemaBuilder.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        when(protoMessageParser.getDescriptorMap()).thenReturn(newDescriptor);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaBuilder.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaBuilder,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        Field field = MaxComputeSchemaCache.class.getDeclaredField("maxComputeSchema");
        field.setAccessible(true);
        field.set(maxComputeSchemaCache, mockedMaxComputeSchema);

        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();

        verify(maxComputeClient, Mockito.times(0))
                .createOrUpdateTable(Mockito.any());
        assertEquals(mockedMaxComputeSchema, maxComputeSchema);
    }

    /**
     * Verifies that a Stencil schema update triggers a fresh table upsert.
     *
     * <p>Given a cache in {@code LOG_MESSAGE} mode that has already built its schema once, when
     * {@link MaxComputeSchemaCache#onSchemaUpdate(Map)} is invoked with a new descriptor map, then the table is
     * created or updated a second time, for a total of two upserts.</p>
     *
     * @throws OdpsException never in this test; declared because the upsert path may throw it
     */
    @Test
    public void shouldUpdateSchemaBasedOnNewDescriptor() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = Mockito.mock(MaxComputeSchemaBuilder.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaBuilder.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaBuilder,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doNothing()
                .when(maxComputeClient)
                .createOrUpdateTable(Mockito.any());
        Mockito.doReturn(Mockito.mock(TableSchema.class))
                .when(maxComputeClient)
                .getLatestTableSchema();

        maxComputeSchemaCache.getMaxComputeSchema();
        maxComputeSchemaCache.onSchemaUpdate(newDescriptor);

        verify(maxComputeClient, Mockito.times(2))
                .createOrUpdateTable(Mockito.any());
    }

    /**
     * Verifies that schema updates use the key class descriptor when running in {@code LOG_KEY} mode.
     *
     * <p>Given a cache configured in {@link SinkConnectorSchemaMessageMode#LOG_KEY} mode that has already built
     * its schema once, when {@link MaxComputeSchemaCache#onSchemaUpdate(Map)} is invoked with a new descriptor
     * map keyed by the configured key class, then the table is created or updated a second time, for a total of
     * two upserts.</p>
     *
     * @throws OdpsException never in this test; declared because the upsert path may throw it
     */
    @Test
    public void shouldUpdateSchemaUsingLogKeyBasedOnNewDescriptor() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = Mockito.mock(MaxComputeSchemaBuilder.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaBuilder.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_KEY);
        when(sinkConfig.getSinkConnectorSchemaProtoKeyClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaBuilder,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        Mockito.doNothing()
                .when(maxComputeClient)
                .createOrUpdateTable(Mockito.any());
        Mockito.doReturn(Mockito.mock(TableSchema.class))
                .when(maxComputeClient)
                .getLatestTableSchema();

        maxComputeSchemaCache.getMaxComputeSchema();
        maxComputeSchemaCache.onSchemaUpdate(newDescriptor);

        verify(maxComputeClient, Mockito.times(2))
                .createOrUpdateTable(Mockito.any());
    }

    /**
     * Verifies that a failing table upsert is surfaced as a {@link MaxComputeTableOperationException}.
     *
     * <p>Given a {@link MaxComputeClient} whose {@code createOrUpdateTable} is stubbed to throw an
     * {@link OdpsException}, when {@link MaxComputeSchemaCache#getMaxComputeSchema()} triggers the schema build,
     * then the {@link OdpsException} is wrapped and rethrown as a {@link MaxComputeTableOperationException}, as
     * asserted by the {@code expected} attribute of the {@link Test} annotation.</p>
     *
     * @throws OdpsException never in this test; declared because the upsert path may throw it
     */
    @Test(expected = MaxComputeTableOperationException.class)
    public void shouldThrowMaxComputeTableOperationExceptionWhenUpsertIsFailing() throws OdpsException {
        Map<String, Descriptors.Descriptor> newDescriptor = new HashMap<>();
        newDescriptor.put("class", Mockito.mock(Descriptors.Descriptor.class));
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = Mockito.mock(MaxComputeSchemaBuilder.class);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        MaxComputeSchema mockedMaxComputeSchema = new MaxComputeSchema(null, null);
        when(maxComputeSchemaBuilder.build(Mockito.any()))
                .thenReturn(mockedMaxComputeSchema);
        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(sinkConfig.getSinkConnectorSchemaProtoMessageClass())
                .thenReturn("class");
        MaxComputeClient maxComputeClient = Mockito.spy(MaxComputeClient.class);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        MaxComputeSchemaCache maxComputeSchemaCache = new MaxComputeSchemaCache(
                maxComputeSchemaBuilder,
                sinkConfig,
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeClient
        );
        maxComputeSchemaCache.setMessageParser(protoMessageParser);
        doThrow(new OdpsException("Invalid schema"))
                .when(maxComputeClient)
                .createOrUpdateTable(Mockito.any());

        maxComputeSchemaCache.getMaxComputeSchema();
        maxComputeSchemaCache.onSchemaUpdate(newDescriptor);
    }

}
