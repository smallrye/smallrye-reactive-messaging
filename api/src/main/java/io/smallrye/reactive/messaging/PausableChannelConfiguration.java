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
     * The name of the channel.
     */
    String name();

    /**
     * Whether the channel is paused at subscribe time.
     */
    boolean initiallyPaused();

}
