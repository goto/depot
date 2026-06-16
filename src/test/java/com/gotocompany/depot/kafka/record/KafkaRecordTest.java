package com.gotocompany.depot.kafka.record;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KafkaRecordTest {

    @Test
    public void shouldCreateValidRecord() {
        byte[] key = new byte[]{1, 2};
        byte[] value = new byte[]{3, 4};
        KafkaRecord record = KafkaRecord.validRecord(3L, key, value, "{topic=test}");
        assertTrue(record.isValid());
        assertEquals(3L, record.getIndex());
        assertArrayEquals(key, record.getKey());
        assertArrayEquals(value, record.getValue());
        assertNull(record.getErrorInfo());
        assertEquals("{topic=test}", record.getMetadata());
        assertEquals("KafkaRecord: Index: 3, Metadata: {topic=test}", record.toString());
    }

    @Test
    public void shouldCreateInvalidRecord() {
        ErrorInfo errorInfo = new ErrorInfo(new IOException("parse failure"), ErrorType.DESERIALIZATION_ERROR);
        KafkaRecord record = KafkaRecord.invalidRecord(7L, errorInfo, "{topic=test}");
        assertFalse(record.isValid());
        assertEquals(7L, record.getIndex());
        assertNull(record.getKey());
        assertNull(record.getValue());
        assertEquals(errorInfo, record.getErrorInfo());
        assertEquals("{topic=test}", record.getMetadata());
    }
}
