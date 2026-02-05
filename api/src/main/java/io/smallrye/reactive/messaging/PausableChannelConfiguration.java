package io.smallrye.reactive.messaging;

/**
 * A channel that can be paused and resumed.
 */
public interface PausableChannelConfiguration {

    /**
     * The name of the property to configure whether the channel is pausable.
     */
    String PAUSABLE_PROPERTY = "pausable";

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
     * The name of the channel.
     */
    String name();

    /**
     * Whether the channel is paused at subscribe time.
     * Default is {@code false}.
     */
    boolean initiallyPaused();

    /**
     * The maximum buffer size for buffered items when the channel is paused.
     */
    Integer bufferSize();

    /**
     * Whether the subscription is done after the channel is paused.
     * Default is {@code false}.
     */
    boolean lateSubscription();

    /**
     * The buffer strategy if previously requested items are received when the channel is paused.
     * Default is {@link PausableBufferStrategy#BUFFER}.
     */
    PausableBufferStrategy bufferStrategy();

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
