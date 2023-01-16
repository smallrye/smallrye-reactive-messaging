package io.smallrye.reactive.messaging.kafka.impl;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;

/**
 * This class duplicate all configuration of the <code>KafkaConnectorIncomingConfiguration</code> that needs to be
 * access at runtime (as opposed to configuration time), meaning all the items that are access for each message.
 *
 * Accessing configuration items via microprofile API is costly so we are better to cache them.
 */
public class RuntimeKafkaSourceConfiguration {

    private final String channel;
    private final boolean pauseIfNoRequests;
    private final int maxQueueSizeFactor;
    private final int retryAttempts;
    private final boolean retry;
    private final int retryMaxWait;
    private final int maxPollRecords;

    private final int closeTimeout;

    public RuntimeKafkaSourceConfiguration(String channel, boolean pauseIfNoRequests,
            int maxQueueSizeFactor, int retryAttempts, boolean retry, int retryMaxWait, int maxPollRecords,
            int closeTimeout) {
        this.channel = channel;
        this.pauseIfNoRequests = pauseIfNoRequests;
        this.maxQueueSizeFactor = maxQueueSizeFactor;
        this.retryAttempts = retryAttempts;
        this.retry = retry;
        this.retryMaxWait = retryMaxWait;
        this.maxPollRecords = maxPollRecords;
        this.closeTimeout = closeTimeout;
    }

    public static RuntimeKafkaSourceConfiguration buildFromConfiguration(KafkaConnectorIncomingConfiguration config) {
        return new RuntimeKafkaSourceConfiguration(
                config.getChannel(),
                config.getPauseIfNoRequests(),
                config.getMaxQueueSizeFactor(),
                config.getRetryAttempts(),
                config.getRetry(),
                config.getRetryMaxWait(),
                config.config().getOptionalValue(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.class).orElse(500),
                config.config().getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class).orElse(1000));
    }

    public String getChannel() {
        return channel;
    }

    public boolean getPauseIfNoRequests() {
        return pauseIfNoRequests;
    }

    public int getMaxQueueSizeFactor() {
        return maxQueueSizeFactor;
    }

    public int getRetryAttempts() {
        return retryAttempts;
    }

    public boolean getRetry() {
        return retry;
    }

    public int getRetryMaxWait() {
        return retryMaxWait;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public int getCloseTimeout() {
        return closeTimeout;
    }
}
