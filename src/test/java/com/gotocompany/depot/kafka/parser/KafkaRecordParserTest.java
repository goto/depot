package com.gotocompany.depot.kafka.parser;

import com.gotocompany.depot.TestKafkaOutputKey;
import com.gotocompany.depot.TestKafkaOutputMessage;
import com.gotocompany.depot.TestKafkaSourceMessage;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunctionCache;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunctionFactory;
import com.gotocompany.depot.kafka.record.KafkaRecord;
import com.gotocompany.depot.kafka.serializer.ProtoKafkaMessageSerializer;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaRecordParserTest {

    private static final String SCHEMA_CLASS = "com.gotocompany.depot.TestKafkaSourceMessage";

    @Mock
    private MessageParser messageParser;
    @Mock
    private ParsedMessage parsedMessage;
    private ProtoMappingFunctionCache mappingFunctionCache;
    private KafkaRecordParser recordParser;
    private TestKafkaSourceMessage sourceMessage;
    private Message message;

    @Before
    public void setup() throws IOException {
        sourceMessage = TestKafkaSourceMessage.newBuilder()
                .setOrderNumber(93L)
                .setAccountGoId("user-1")
                .build();
        message = new Message(null, sourceMessage.toByteArray());
        mappingFunctionCache = new ProtoMappingFunctionCache();
        Map<String, String> protoMapping = new LinkedHashMap<>();
        protoMapping.put("order_id", "\"wee\" + string(source.order_number)");
        protoMapping.put("user_id", "source.account_go_id");
        protoMapping.put("order_number", "source.order_number");
        mappingFunctionCache.set(new ProtoMappingFunctionFactory().create(
                TestKafkaSourceMessage.getDescriptor(),
                TestKafkaOutputMessage.getDescriptor(),
                TestKafkaOutputKey.getDescriptor(),
                protoMapping));
        recordParser = new KafkaRecordParser(messageParser, mappingFunctionCache, new ProtoKafkaMessageSerializer(),
                new Tuple<>(SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS));
    }

    @Test
    public void shouldConvertMessagesToValidKafkaRecords() throws IOException {
        when(messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS)).thenReturn(parsedMessage);
        when(parsedMessage.getRaw()).thenReturn(sourceMessage);
        List<KafkaRecord> records = recordParser.convert(Collections.singletonList(message));
        assertEquals(1, records.size());
        KafkaRecord record = records.get(0);
        assertTrue(record.isValid());
        assertEquals(0L, record.getIndex());
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(record.getValue());
        TestKafkaOutputKey outputKey = TestKafkaOutputKey.parseFrom(record.getKey());
        assertEquals("wee93", outputMessage.getOrderId());
        assertEquals("user-1", outputMessage.getUserId());
        assertEquals("wee93", outputKey.getOrderId());
        assertEquals(93L, outputKey.getOrderNumber());
    }

    @Test
    public void shouldCreateRecordWithNullKeyWhenKeyMappingIsNotConfigured() throws IOException {
        mappingFunctionCache.set(new ProtoMappingFunctionFactory().create(
                TestKafkaSourceMessage.getDescriptor(),
                TestKafkaOutputMessage.getDescriptor(),
                null,
                Collections.singletonMap("user_id", "source.account_go_id")));
        when(messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS)).thenReturn(parsedMessage);
        when(parsedMessage.getRaw()).thenReturn(sourceMessage);
        List<KafkaRecord> records = recordParser.convert(Collections.singletonList(message));
        assertTrue(records.get(0).isValid());
        assertNull(records.get(0).getKey());
        assertEquals("user-1", TestKafkaOutputMessage.parseFrom(records.get(0).getValue()).getUserId());
    }

    @Test
    public void shouldCollectMappingEvaluationFailuresAsInvalidMessageErrors() throws IOException {
        mappingFunctionCache.set(new ProtoMappingFunctionFactory().create(
                TestKafkaSourceMessage.getDescriptor(),
                TestKafkaOutputMessage.getDescriptor(),
                null,
                Collections.singletonMap("primary_order_id", "source.order_list[4]")));
        when(messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS)).thenReturn(parsedMessage);
        when(parsedMessage.getRaw()).thenReturn(sourceMessage);
        List<KafkaRecord> records = recordParser.convert(Collections.singletonList(message));
        assertFalse(records.get(0).isValid());
        assertEquals(ErrorType.INVALID_MESSAGE_ERROR, records.get(0).getErrorInfo().getErrorType());
    }

    @Test
    public void shouldReturnEmptyRecordsForEmptyMessages() {
        List<KafkaRecord> records = recordParser.convert(Collections.emptyList());
        assertTrue(records.isEmpty());
    }

    @Test
    public void shouldCollectKeyMappingEvaluationFailuresAsInvalidMessageErrors() throws IOException {
        Map<String, String> protoMapping = new LinkedHashMap<>();
        protoMapping.put("user_id", "source.account_go_id");
        protoMapping.put("order_number", "source.order_list[0]");
        mappingFunctionCache.set(new ProtoMappingFunctionFactory().create(
                TestKafkaSourceMessage.getDescriptor(),
                TestKafkaOutputMessage.getDescriptor(),
                TestKafkaOutputKey.getDescriptor(),
                protoMapping));
        when(messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS)).thenReturn(parsedMessage);
        when(parsedMessage.getRaw()).thenReturn(sourceMessage);
        List<KafkaRecord> records = recordParser.convert(Collections.singletonList(message));
        assertFalse(records.get(0).isValid());
        assertEquals(ErrorType.INVALID_MESSAGE_ERROR, records.get(0).getErrorInfo().getErrorType());
    }

    @Test
    public void shouldProduceEmptyKeyBytesWhenNoMappedFieldsExistInKeyProto() throws IOException {
        mappingFunctionCache.set(new ProtoMappingFunctionFactory().create(
                TestKafkaSourceMessage.getDescriptor(),
                TestKafkaOutputMessage.getDescriptor(),
                TestKafkaOutputKey.getDescriptor(),
                Collections.singletonMap("user_id", "source.account_go_id")));
        when(messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS)).thenReturn(parsedMessage);
        when(parsedMessage.getRaw()).thenReturn(sourceMessage);
        List<KafkaRecord> records = recordParser.convert(Collections.singletonList(message));
        assertTrue(records.get(0).isValid());
        assertEquals(0, records.get(0).getKey().length);
    }

    @Test
    public void shouldCollectErrorsForFailedMessagesAndConvertOthers() throws IOException {
        TestKafkaSourceMessage secondSourceMessage = TestKafkaSourceMessage.newBuilder().setOrderNumber(94L).build();
        Message emptyMessage = new Message(null, null);
        Message invalidMessage = new Message(null, new byte[]{1, 2, 3});
        Message badConfigMessage = new Message(null, secondSourceMessage.toByteArray());
        when(messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS)).thenReturn(parsedMessage);
        when(parsedMessage.getRaw()).thenReturn(sourceMessage);
        when(messageParser.parse(emptyMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS))
                .thenThrow(new EmptyMessageException());
        when(messageParser.parse(invalidMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS))
                .thenThrow(new IOException("invalid proto bytes"));
        when(messageParser.parse(badConfigMessage, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS))
                .thenThrow(new IllegalArgumentException("invalid configuration"));
        List<KafkaRecord> records = recordParser.convert(Arrays.asList(message, emptyMessage, invalidMessage, badConfigMessage));
        assertEquals(4, records.size());
        assertTrue(records.get(0).isValid());
        assertFalse(records.get(1).isValid());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, records.get(1).getErrorInfo().getErrorType());
        assertEquals(1L, records.get(1).getIndex());
        assertFalse(records.get(2).isValid());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, records.get(2).getErrorInfo().getErrorType());
        assertEquals(2L, records.get(2).getIndex());
        assertFalse(records.get(3).isValid());
        assertEquals(ErrorType.SINK_UNKNOWN_ERROR, records.get(3).getErrorInfo().getErrorType());
        assertEquals(3L, records.get(3).getIndex());
    }

    @Test
    public void shouldClassifyUnexpectedRuntimeExceptionsAsUnknownError() throws IOException {
        when(messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, SCHEMA_CLASS))
                .thenThrow(new IllegalStateException("unexpected parser state"));
        List<KafkaRecord> records = recordParser.convert(Collections.singletonList(message));
        assertEquals(1, records.size());
        assertFalse(records.get(0).isValid());
        assertEquals(ErrorType.SINK_UNKNOWN_ERROR, records.get(0).getErrorInfo().getErrorType());
        assertEquals(0L, records.get(0).getIndex());
    }
}
