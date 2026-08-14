package com.gotocompany.depot.bigtable.parser;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.TestMessage;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link BigTableRowKeyParser}, which renders a Bigtable row key from a
 * {@link Template} and a {@link ParsedMessage}.
 *
 * <p>Each test seeds the {@code SINK_BIGTABLE_ROW_KEY_TEMPLATE} and proto-message-class system
 * properties, loads a {@link BigTableSinkConfig}, parses a {@link TestMessage} through a real
 * {@link ProtoMessageParser} backed by a no-op StatsD client, then asserts either the row key
 * produced by {@link BigTableRowKeyParser#parse(ParsedMessage)} or the
 * {@link InvalidTemplateException} raised when building an invalid {@link Template}.</p>
 */
public class BigTableRowKeyParserTest {

    /**
     * Verifies that a parameterised template is rendered using fields from the parsed message.
     *
     * <p>Given the template {@code "row-%s$key#%s*test,order_number,order_details"} and a message with
     * {@code order_number=xyz-order} and {@code order_details=eureka}, when the parsed message is
     * passed to {@link BigTableRowKeyParser#parse(ParsedMessage)}, then the row key resolves to
     * {@code "row-xyz-order$key#eureka*test"}.</p>
     *
     * @throws IOException if message parsing fails
     * @throws InvalidTemplateException if the template is not valid
     */
    @Test
    public void shouldReturnParsedRowKeyForValidParameterisedTemplate() throws IOException, InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-%s$key#%s*test,order_number,order_details");
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

        ProtoMessageParser messageParser = new ProtoMessageParser(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);

        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("eureka")
                .build()
                .toByteArray();
        Message message = new Message(null, logMessage);
        ParsedMessage parsedMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());

        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));
        String parsedRowKey = bigTableRowKeyParser.parse(parsedMessage);
        assertEquals("row-xyz-order$key#eureka*test", parsedRowKey);
    }

    /**
     * Verifies that a constant-only template is returned verbatim as the row key.
     *
     * <p>Given the placeholder-free template {@code "row-key#constant$String"}, when a parsed message
     * is rendered, then the resulting row key equals the template unchanged.</p>
     *
     * @throws IOException if message parsing fails
     * @throws InvalidTemplateException if the template is not valid
     */
    @Test
    public void shouldReturnTheRowKeySameAsTemplateWhenTemplateIsValidAndContainsOnlyConstantStrings() throws IOException, InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key#constant$String");
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

        ProtoMessageParser messageParser = new ProtoMessageParser(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);

        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("eureka")
                .build()
                .toByteArray();
        Message message = new Message(null, logMessage);
        ParsedMessage parsedMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());

        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));
        String parsedRowKey = bigTableRowKeyParser.parse(parsedMessage);
        assertEquals("row-key#constant$String", parsedRowKey);
    }

    /**
     * Verifies that a template with a placeholder but no field argument is rejected.
     *
     * <p>Given the malformed template {@code "row-key%s"}, when a {@link Template} is constructed from
     * it, then an {@link InvalidTemplateException} with the message
     * {@code "Template is not valid, variables=1, validArgs=1, values=0"} is thrown.</p>
     */
    @Test
    public void shouldThrowErrorForInvalidTemplate() {
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key%s");
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

        InvalidTemplateException illegalArgumentException = Assertions.assertThrows(InvalidTemplateException.class, () -> new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate())));
        assertEquals("Template is not valid, variables=1, validArgs=1, values=0", illegalArgumentException.getMessage());
    }

}
