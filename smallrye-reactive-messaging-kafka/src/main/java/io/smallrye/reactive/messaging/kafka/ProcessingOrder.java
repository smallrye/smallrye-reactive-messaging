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
    ORDERED_BY_KEY,

    /**
     * Messages from the same topic-partition are processed sequentially
     */
    ORDERED_BY_PARTITION;

    public static ProcessingOrder of(String order) {
        if (order == null || order.isBlank() || "unordered".equalsIgnoreCase(order)) {
            return UNORDERED;
        } else if ("ordered_by_key".equalsIgnoreCase(order)) {
            return ORDERED_BY_KEY;
        } else if ("ordered_by_partition".equalsIgnoreCase(order)) {
            return ORDERED_BY_PARTITION;
        } else {
            throw new IllegalArgumentException(
                    "Invalid processing order: " + order
                            + ". Supported values are: unordered, ordered_by_key, ordered_by_partition");
        }
    }
}
