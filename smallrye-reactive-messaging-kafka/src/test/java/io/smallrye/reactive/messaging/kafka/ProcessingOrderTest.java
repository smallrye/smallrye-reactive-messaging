package io.smallrye.reactive.messaging.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ProcessingOrderTest {

    @Test
    public void testParseUnordered() {
        assertEquals(ProcessingOrder.UNORDERED, ProcessingOrder.of(null));
        assertEquals(ProcessingOrder.UNORDERED, ProcessingOrder.of(""));
        assertEquals(ProcessingOrder.UNORDERED, ProcessingOrder.of("  "));
    }

    @Test
    public void testParseOrderedByKey() {
        assertEquals(ProcessingOrder.KEY, ProcessingOrder.of("key"));
        assertEquals(ProcessingOrder.KEY, ProcessingOrder.of("KEY"));
        assertEquals(ProcessingOrder.KEY, ProcessingOrder.of("KeY"));
    }

    @Test
    public void testParseOrderedByPartition() {
        assertEquals(ProcessingOrder.PARTITION, ProcessingOrder.of("partition"));
        assertEquals(ProcessingOrder.PARTITION, ProcessingOrder.of("PARTITION"));
        assertEquals(ProcessingOrder.PARTITION, ProcessingOrder.of("PaRtItIoN"));
    }

    @Test
    public void testInvalidValue() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ProcessingOrder.of("invalid"));
        assertEquals("Invalid processing order: invalid. Supported values are: key, partition",
                exception.getMessage());
    }

}
