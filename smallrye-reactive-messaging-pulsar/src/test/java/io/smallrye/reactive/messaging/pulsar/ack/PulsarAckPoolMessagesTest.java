package io.smallrye.reactive.messaging.pulsar.ack;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarAckPoolMessagesTest extends PulsarAckTest {

    MapBasedConfig config() {
        return super.config()
                .with("mp.messaging.incoming.data.poolMessages", true);
    }

}
