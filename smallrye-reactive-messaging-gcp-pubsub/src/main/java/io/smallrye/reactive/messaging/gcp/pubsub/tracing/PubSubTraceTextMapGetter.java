package io.smallrye.reactive.messaging.gcp.pubsub.tracing;

import java.util.Collections;
import java.util.Map;

import io.opentelemetry.context.propagation.TextMapGetter;

public enum PubSubTraceTextMapGetter implements TextMapGetter<PubSubTrace> {
    INSTANCE;

    @Override
    public Iterable<String> keys(final PubSubTrace carrier) {
        Map<String, String> attributes = carrier.getAttributes();
        if (attributes != null) {
            return attributes.keySet();
        }
        return Collections.emptyList();
    }

    @Override
    public String get(final PubSubTrace carrier, final String key) {
        if (carrier != null) {
            Map<String, String> attributes = carrier.getAttributes();
            if (attributes != null) {
                return attributes.get(key);
            }
        }
        return null;
    }
}
