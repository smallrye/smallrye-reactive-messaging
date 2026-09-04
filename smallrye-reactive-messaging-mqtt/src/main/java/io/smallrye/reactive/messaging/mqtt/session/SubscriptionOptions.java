package io.smallrye.reactive.messaging.mqtt.session;

/**
 * Holds subscription options including MQTT v5 subscription options.
 */
public class SubscriptionOptions {

    private final RequestedQoS qos;
    private final boolean noLocal;
    private final boolean retainAsPublished;
    private final int retainHandling;
    private final Integer subscriptionIdentifier;

    public SubscriptionOptions(RequestedQoS qos) {
        this(qos, false, false, 0, null);
    }

    public SubscriptionOptions(RequestedQoS qos, boolean noLocal, boolean retainAsPublished, int retainHandling) {
        this(qos, noLocal, retainAsPublished, retainHandling, null);
    }

    public SubscriptionOptions(RequestedQoS qos, boolean noLocal, boolean retainAsPublished, int retainHandling,
            Integer subscriptionIdentifier) {
        this.qos = qos;
        this.noLocal = noLocal;
        this.retainAsPublished = retainAsPublished;
        this.retainHandling = retainHandling;
        this.subscriptionIdentifier = subscriptionIdentifier;
    }

    public RequestedQoS getQos() {
        return qos;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public boolean isRetainAsPublished() {
        return retainAsPublished;
    }

    public int getRetainHandling() {
        return retainHandling;
    }

    /**
     * @return the subscription identifier (MQTT 5.0), or {@code null} if not set
     */
    public Integer getSubscriptionIdentifier() {
        return subscriptionIdentifier;
    }

    /**
     * @return {@code true} if any MQTT v5 subscription option is set to a non-default value
     */
    public boolean hasV5Options() {
        return noLocal || retainAsPublished || retainHandling != 0 || subscriptionIdentifier != null;
    }
}
