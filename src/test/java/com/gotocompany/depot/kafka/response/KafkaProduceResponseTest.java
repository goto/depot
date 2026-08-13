package com.gotocompany.depot.kafka.response;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KafkaProduceResponseTest {

    @Test
    public void shouldCreateSuccessResponse() {
        KafkaProduceResponse response = KafkaProduceResponse.success();
        assertFalse(response.isFailed());
        assertNull(response.getError());
        assertEquals("success", response.getMessage());
    }

    @Test
    public void shouldCreateFailureResponse() {
        TimeoutException error = new TimeoutException("broker timed out");
        KafkaProduceResponse response = KafkaProduceResponse.failure(error);
        assertTrue(response.isFailed());
        assertEquals(error, response.getError());
        assertEquals("broker timed out", response.getMessage());
    }
}
