package io.smallrye.reactive.messaging.kafka.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionProxyTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

// TODO this test does not work yet
@Disabled
public class ProducerRetryTest extends KafkaCompanionProxyTestBase {

    private KafkaMapBasedConfig getBaseConfig() {
        return kafkaConfig("mp.messaging.outgoing.kafka")
                .put("topic", topic)
                .put("partition", 0)
                .put("key.serializer", StringSerializer.class.getName())
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("max.in.flight.requests.per.connection", 1)
                .put("acks", "1");
    }

    @Test
    public void testProduceWithRetryWhenBrokerUnavailable() throws InterruptedException {
        MapBasedConfig config = getBaseConfig()
                //                .put("linger.ms", 1000)
                //                .put("max.block.ms", 1000)
                .put("delivery.timeout.ms", 1000)
                .put("request.timeout.ms", 200)
                //                .put("retries", 10)
                .build();

        EmitterProducer bean = runApplication(config, EmitterProducer.class);

        // produce 10 messages
        for (int i = 0; i < 10; i++) {
            bean.produce(i);
        }
        await().until(() -> isReady());

        disableProxy();

        // produce 10 more
        for (int i = 10; i < 20; i++) {
            bean.produce(i);
        }

        // wait until not alive
        //        await().until(() -> !isReady());

        enableProxy();

        assertThat(companion.consumeIntegers().fromTopics(topic, 20, Duration.ofMinutes(1))
                .awaitCompletion(Duration.ofMinutes(2)).count()).isEqualTo(20);
    }

    @ApplicationScoped
    public static class EmitterProducer {

        @Inject
        @Channel("kafka")
        Emitter<Integer> emitter;

        public void produce(int value) {
            emitter.send(value);
        }
    }
}
