package io.smallrye.reactive.messaging.mqtt.session;

/**
 * The state of a subscription.
 * <p>
 * Subscriptions established when a new topic gets added, or the connection was established. If the subscribe call
 * returns an error for the subscription, the state will remain {@link #FAILED} and it will not try to re-subscribe
 * while the connection is active.
 * <p>
 * When the session (connection) disconnects, all subscriptions will automatically be reset to {@link #UNSUBSCRIBED}.
 */
public enum SubscriptionState {
    /**
     * The topic is not subscribed.
     */
    UNSUBSCRIBED,
    /**
     * The topic is in the process of subscribing.
     */
    SUBSCRIBING,
    /**
     * The topic is subscribed.
     */
    SUBSCRIBED,
    /**
     * The topic could not be subscribed.
     */
    FAILED,
}
