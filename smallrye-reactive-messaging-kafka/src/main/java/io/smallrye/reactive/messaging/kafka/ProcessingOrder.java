package io.smallrye.reactive.messaging.kafka;

/**
 * Processing order modes for the throttled commit strategy.
 * <p>
 * These modes control which messages can be processed concurrently while maintaining
 * ordering guarantees within groups (keys or partitions).
 */
public enum ProcessingOrder {
    UNORDERED,
    /**
     * Messages with the same key from the same topic-partition are processed sequentially,
     */
    KEY,

    /**
     * Messages from the same topic-partition are processed sequentially
     */
    PARTITION;

    public static ProcessingOrder of(String order) {
        if ("key".equalsIgnoreCase(order)) {
            return KEY;
        } else if ("partition".equalsIgnoreCase(order)) {
            return PARTITION;
        } else if (order == null || order.isBlank()) {
            return UNORDERED;
        } else {
            throw new IllegalArgumentException("Invalid processing order: " + order
                    + ". Supported values are: key, partition");
        }
    }
}
