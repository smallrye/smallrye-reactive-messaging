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
        assertEquals(ProcessingOrder.UNORDERED, ProcessingOrder.of("unordered"));
        assertEquals(ProcessingOrder.UNORDERED, ProcessingOrder.of("UNORDERED"));
        assertEquals(ProcessingOrder.UNORDERED, ProcessingOrder.of("UnOrDeReD"));
    }

    @Test
    public void testParseOrderedByKey() {
        assertEquals(ProcessingOrder.ORDERED_BY_KEY, ProcessingOrder.of("ordered_by_key"));
        assertEquals(ProcessingOrder.ORDERED_BY_KEY, ProcessingOrder.of("ORDERED_BY_KEY"));
        assertEquals(ProcessingOrder.ORDERED_BY_KEY, ProcessingOrder.of("OrDeReD_bY_KeY"));
    }

    @Test
    public void testParseOrderedByPartition() {
        assertEquals(ProcessingOrder.ORDERED_BY_PARTITION, ProcessingOrder.of("ordered_by_partition"));
        assertEquals(ProcessingOrder.ORDERED_BY_PARTITION, ProcessingOrder.of("ORDERED_BY_PARTITION"));
        assertEquals(ProcessingOrder.ORDERED_BY_PARTITION, ProcessingOrder.of("OrDeReD_bY_PaRtItIoN"));
    }

    @Test
    public void testInvalidValue() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ProcessingOrder.of("invalid"));
        assertEquals("Invalid processing order: invalid. Supported values are: unordered, ordered_by_key, ordered_by_partition",
                exception.getMessage());
    }

    @Test
    public void testInvalidValueWithHyphen() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ProcessingOrder.of("ordered-by-key"));
        assertEquals(
                "Invalid processing order: ordered-by-key. Supported values are: unordered, ordered_by_key, ordered_by_partition",
                exception.getMessage());
    }
}
