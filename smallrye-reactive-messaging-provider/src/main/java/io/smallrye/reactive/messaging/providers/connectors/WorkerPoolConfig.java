package io.smallrye.reactive.messaging.providers.connectors;

import java.time.Duration;

public class WorkerPoolConfig {

    private final Integer maxConcurrency;
    private final Duration shutdownTimeout;
    private final Duration shutdownCheckInterval;

    public WorkerPoolConfig(int maxConcurrency, int shutdownTimeoutMs, int shutdownCheckIntervalMs) {
        this(maxConcurrency, Duration.ofMillis(shutdownTimeoutMs), Duration.ofMillis(shutdownCheckIntervalMs));
    }

    public WorkerPoolConfig(int maxConcurrency, Duration shutdownTimeout, Duration shutdownCheckInterval) {
        this.maxConcurrency = maxConcurrency;
        this.shutdownTimeout = shutdownTimeout;
        this.shutdownCheckInterval = shutdownCheckInterval;
    }

    public int maxConcurrency() {
        return maxConcurrency;
    }

    /**
     * The shutdown timeout. If all pending work has not been completed by this time
     * then any pending tasks will be interrupted, and the shutdown process will continue
     */
    public Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    /**
     * The frequency at which the status of the executor service should be checked during shutdown.
     */
    public Duration shutdownCheckInterval() {
        return shutdownCheckInterval;
    }

}
