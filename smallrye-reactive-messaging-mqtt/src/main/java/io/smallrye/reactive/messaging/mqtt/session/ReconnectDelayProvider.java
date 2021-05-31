package io.smallrye.reactive.messaging.mqtt.session;

import java.time.Duration;

public interface ReconnectDelayProvider {

    Duration nextDelay();

    void reset();

}
