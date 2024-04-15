package io.smallrye.reactive.messaging.pulsar.fault;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarNackPoolMessagesTest extends PulsarNackTest {

    MapBasedConfig config() {
        return super.config()
                .with("mp.messaging.incoming.data.poolMessages", true);
    }

}
