package com.gotocompany.depot.kafka.mapping;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ProtoMappingFunctionCacheTest {

    @Test
    public void shouldThrowWhenMappingFunctionIsNotInitialized() {
        ProtoMappingFunctionCache cache = new ProtoMappingFunctionCache();
        IllegalStateException exception = assertThrows(IllegalStateException.class, cache::get);
        assertEquals("proto mapping function has not been initialized for the kafka sink", exception.getMessage());
    }

    @Test
    public void shouldReturnTheLatestMappingFunction() {
        ProtoMappingFunctionCache cache = new ProtoMappingFunctionCache();
        ProtoMappingFunction firstFunction = Mockito.mock(ProtoMappingFunction.class);
        ProtoMappingFunction secondFunction = Mockito.mock(ProtoMappingFunction.class);
        cache.set(firstFunction);
        assertEquals(firstFunction, cache.get());
        cache.set(secondFunction);
        assertEquals(secondFunction, cache.get());
    }
}
