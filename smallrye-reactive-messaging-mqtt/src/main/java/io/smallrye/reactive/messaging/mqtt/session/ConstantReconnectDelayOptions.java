package io.smallrye.reactive.messaging.mqtt.session;

import java.time.Duration;

public class ConstantReconnectDelayOptions implements ReconnectDelayOptions {

    private static final Duration DEFAULT_DELAY = Duration.ofSeconds(10);

    private Duration delay = DEFAULT_DELAY;

    public ConstantReconnectDelayOptions() {
    }

    public ConstantReconnectDelayOptions setDelay(Duration delay) {
        this.delay = delay;
        return this;
    }

    public Duration getDelay() {
        return this.delay;
    }

    @Override
    public ReconnectDelayProvider createProvider() {

        final Duration delay = this.delay;

        return new ReconnectDelayProvider() {

            @Override
            public Duration nextDelay() {
                return delay;
            }

            @Override
            public void reset() {
                // no-op
            }
        };

    }

    @Override
    public ReconnectDelayOptions copy() {
        ConstantReconnectDelayOptions result = new ConstantReconnectDelayOptions();
        result.delay = this.delay;
        return result;
    }
}
