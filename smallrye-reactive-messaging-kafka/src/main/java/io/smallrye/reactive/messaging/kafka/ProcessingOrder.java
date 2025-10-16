package io.smallrye.reactive.messaging.kafka;

public enum ProcessingOrder {
    UNORDERED,
    ORDERED_BY_KEY,
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
