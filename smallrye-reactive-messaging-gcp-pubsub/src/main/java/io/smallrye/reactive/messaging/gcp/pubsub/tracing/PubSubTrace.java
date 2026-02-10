package io.smallrye.reactive.messaging.gcp.pubsub.tracing;

import java.util.Map;

/**
 * Carrier object for GCP Pub/Sub tracing propagation.
 * Holds message metadata and attributes map used for injecting/extracting trace context.
 */
public class PubSubTrace {
    private final String topic;
    private final String subscription;
    private final Map<String, String> attributes;

    private PubSubTrace(final String topic, final String subscription, final Map<String, String> attributes) {
        this.topic = topic;
        this.subscription = subscription;
        this.attributes = attributes;
    }

    public String getTopic() {
        return topic;
    }

    public String getSubscription() {
        return subscription;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public static PubSubTrace traceTopic(final String topic, final Map<String, String> attributes) {
        return new PubSubTrace(topic, null, attributes);
    }

    public static PubSubTrace traceSubscription(final String topic, final String subscription,
            final Map<String, String> attributes) {
        return new PubSubTrace(topic, subscription, attributes);
    }
}
