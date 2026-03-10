package io.smallrye.reactive.messaging.gcp.pubsub.tracing;

import java.util.Map;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum PubSubTraceTextMapSetter implements TextMapSetter<PubSubTrace> {
    INSTANCE;

    @Override
    public void set(final PubSubTrace carrier, final String key, final String value) {
        if (carrier != null) {
            Map<String, String> attributes = carrier.getAttributes();
            if (attributes != null) {
                attributes.put(key, value);
            }
        }
    }
}
