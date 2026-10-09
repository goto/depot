package com.gotocompany.depot.exception;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ProtoFieldConversionExceptionTest {

    @Test
    public void shouldBuildFieldPathFromLeafToRoot() {
        IllegalStateException cause = new IllegalStateException("empty struct");
        ProtoFieldConversionException leaf = ProtoFieldConversionException.wrap("leaf", 2, 3, cause);
        ProtoFieldConversionException mid = ProtoFieldConversionException.wrap("middle", 0, 1, leaf);
        ProtoFieldConversionException root = ProtoFieldConversionException.wrap("root", 5, -1, mid);

        assertEquals("root.middle.leaf", root.getFieldPath());
        assertSame(cause, root.getCause());
        assertTrue(root.getMessage().contains("index 2 in parent at level 3"));
        assertTrue(root.getMessage().contains("empty struct"));
    }

    @Test
    public void shouldAddMessageContext() {
        ProtoFieldConversionException e = ProtoFieldConversionException.wrap("a", 1, 0, new RuntimeException("x"))
                .withMessageContext("message index 4 in batch, metadata {offset=99}");

        assertTrue(e.getMessage().contains("metadata {offset=99}"));
        assertEquals("a", e.getFieldPath());
    }
}
