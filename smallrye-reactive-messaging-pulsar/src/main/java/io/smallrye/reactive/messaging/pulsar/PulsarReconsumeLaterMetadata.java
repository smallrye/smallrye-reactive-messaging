package io.smallrye.reactive.messaging.pulsar;

import java.time.Duration;
import java.util.Map;

public class PulsarReconsumeLaterMetadata {

    private final Duration delay;
    private final Map<String, String> customProperties;

    public PulsarReconsumeLaterMetadata(Duration delay, Map<String, String> customProperties) {
        this.delay = delay;
        this.customProperties = customProperties;
    }

    public Duration getDelay() {
        return delay;
    }

    public Map<String, String> getCustomProperties() {
        return customProperties;
    }

}
