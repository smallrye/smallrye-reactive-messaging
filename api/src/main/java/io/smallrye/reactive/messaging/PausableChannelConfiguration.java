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
     */
    String PAUSED_PROPERTY = "initially-paused";

    /**
     * The name of the property to configure whether the subscription is postponed to resume if the channel is paused at
     * subscribe time.
     */
    String LATE_SUBSCRIPTION_PROPERTY = "late-subscription";

    /**
     * The name of the property to configure whether to buffer is already requested items when the channel is paused.
     */
    String BUFFER_ALREADY_REQUESTED_PROPERTY = "buffer-already-requested";

    /**
     * The name of the channel.
     */
    String name();

    /**
     * Whether the channel is paused at subscribe time.
     */
    boolean initiallyPaused();

    /**
     * Whether the subscription is done after the channel is paused.
     */
    boolean lateSubscription();

    /**
     * Whether to buffer is already requested items when the channel is paused.
     */
    boolean bufferAlreadyRequested();
}
