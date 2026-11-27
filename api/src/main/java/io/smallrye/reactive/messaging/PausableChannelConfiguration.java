package io.smallrye.reactive.messaging;

/**
 * A channel that can be paused and resumed.
 * Holder for channel config keys used to configure pausable channels.
 */
public interface PausableChannelConfiguration {

    /**
     * The name of the property to configure whether the channel is pausable.
     * <p>
     * When not set, it defaults to the channel's {@link #GRACEFUL_SHUTDOWN_PROPERTY graceful-shutdown} value.
     */
    String PAUSABLE_PROPERTY = "pausable";

    /**
     * The name of the property to configure whether in-flight messages are tracked and drained on shutdown.
     * <p>
     * Set on a channel ({@code mp.messaging.[incoming|outgoing].<channel>.graceful-shutdown}) or on a connector
     * ({@code mp.messaging.connector.<connector>.graceful-shutdown}). When neither is set, it is {@code true}, unless
     * {@link #GRACEFUL_SHUTDOWN_DEFAULT_ENABLED_PROPERTY} is {@code false}.
     * For incoming connector channels, the effective value is resolved by
     * {@link io.smallrye.reactive.messaging.connector.InboundConnector#isGracefulShutdownEnabled(org.eclipse.microprofile.config.Config)},
     * so a connector can declare another default.
     */
    String GRACEFUL_SHUTDOWN_PROPERTY = "graceful-shutdown";

    /**
     * The name of the global property setting the default of {@link #GRACEFUL_SHUTDOWN_PROPERTY}, for the channels
     * that configure it neither on the channel nor on their connector. Defaults to {@code true}. Set it to
     * {@code false} to disable graceful shutdown for every channel that does not enable it explicitly, whatever the
     * connector's default. It is not a kill switch: a {@code graceful-shutdown} property set on a channel or on a
     * connector still applies.
     */
    String GRACEFUL_SHUTDOWN_DEFAULT_ENABLED_PROPERTY = "smallrye.messaging.graceful-shutdown.default.enabled";

    /**
     * The name of the property to configure the maximum time, in milliseconds, to wait for in-flight messages to drain.
     */
    String GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT_PROPERTY = "graceful-shutdown.drain-timeout";

    /**
     * The name of the global property setting the default of {@link #GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT_PROPERTY}, in
     * milliseconds, for the channels that configure it neither on the channel nor on their connector. Defaults to
     * {@code 10000}.
     */
    String GRACEFUL_SHUTDOWN_DEFAULT_DRAIN_TIMEOUT_PROPERTY = "smallrye.messaging.graceful-shutdown.default.drain-timeout";

    /**
     * The name of the property to configure whether the channel is initially paused.
     *
     * @deprecated use {@link #INITIALLY_PAUSED_PROPERTY} instead
     */
    String PAUSED_PROPERTY = "initially-paused";

    /**
     * The name of the property to configure whether the channel is initially paused.
     */
    String INITIALLY_PAUSED_PROPERTY = "pausable.initially-paused";

    /**
     * The name of the property to configure the buffer size.
     */
    String BUFFER_SIZE_PROPERTY = "pausable.buffer-size";

    /**
     * The name of the property to configure whether late subscribers should receive buffered messages.
     */
    String LATE_SUBSCRIPTION_PROPERTY = "pausable.late-subscription";

    /**
     * The name of the property to configure the buffer strategy.
     */
    String BUFFER_STRATEGY_PROPERTY = "pausable.buffer-strategy";

    /**
     * Strategy for handling already requested items that arrive while the channel is paused.
     */
    enum PausableBufferStrategy {
        /**
         * Buffer already requested items that arrive while paused.
         * <p>
         * When the channel is paused, already requested items that arrive from upstream are stored in a buffer
         * and delivered when the channel is resumed.
         * This is the default strategy for pausable channels.
         */
        BUFFER,

        /**
         * Drop already requested items that arrive while paused.
         * <p>
         * When the channel is paused, already requested items that arrive from upstream are silently dropped without buffering.
         * No buffered items are delivered when the channel is resumed (only new items requested after resume).
         */
        DROP,

        /**
         * Ignore pause state for already requested items - items flow through as if channel is not paused.
         * <p>
         * When the channel is paused, already requested items continue to flow downstream normally.
         * The pause state only affects new upstream requests (no new items are requested while paused).
         */
        IGNORE
    }

}
