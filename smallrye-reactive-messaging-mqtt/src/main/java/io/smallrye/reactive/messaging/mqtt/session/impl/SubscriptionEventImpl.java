package io.smallrye.reactive.messaging.mqtt.session.impl;

import java.util.Objects;
import java.util.StringJoiner;

import io.smallrye.reactive.messaging.mqtt.session.SubscriptionEvent;
import io.smallrye.reactive.messaging.mqtt.session.SubscriptionState;

/**
 * An event of a subscription state change.
 */
public class SubscriptionEventImpl implements SubscriptionEvent {
    private final String topic;
    private final SubscriptionState subscriptionState;
    private final Integer qos;

    public SubscriptionEventImpl(final String topic, final SubscriptionState subscriptionState, final Integer qos) {
        this.topic = topic;
        this.subscriptionState = subscriptionState;
        this.qos = qos;
    }

    /**
     * The granted QoS level from the server.
     *
     * @return When the state changed to {@link SubscriptionState#SUBSCRIBED}, it contains the QoS level granted by
     *         the server. Otherwise it will be {@code null}.
     */
    @Override
    public Integer getQos() {
        return this.qos;
    }

    /**
     * The new subscription state.
     *
     * @return The state.
     */
    @Override
    public SubscriptionState getSubscriptionState() {
        return this.subscriptionState;
    }

    /**
     * The name of the topic this change refers to.
     *
     * @return The topic name.
     */
    @Override
    public String getTopic() {
        return this.topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SubscriptionEventImpl that = (SubscriptionEventImpl) o;
        return topic.equals(that.topic) && subscriptionState == that.subscriptionState && Objects.equals(qos, that.qos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, subscriptionState, qos);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SubscriptionEventImpl.class.getSimpleName() + "[", "]")
                .add("topic='" + topic + "'")
                .add("subscriptionState=" + subscriptionState)
                .add("qos=" + qos)
                .toString();
    }
}
