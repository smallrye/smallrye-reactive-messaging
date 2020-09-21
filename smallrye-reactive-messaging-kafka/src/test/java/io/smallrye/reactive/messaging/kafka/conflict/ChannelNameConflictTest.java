package io.smallrye.reactive.messaging.kafka.conflict;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;

/**
 * Reproducer for https://github.com/smallrye/smallrye-reactive-messaging/issues/373.
 */
public class ChannelNameConflictTest extends KafkaTestBase {

    private final static Map<String, Object> CONFLICT = new HashMap<>();

    static {
        CONFLICT.put("mp.messaging.incoming.my-topic.connector", "smallrye-kafka");
        CONFLICT.put("mp.messaging.incoming.my-topic.bootstrap.servers", kafka.getBootstrapServers());
        CONFLICT.put("mp.messaging.incoming.my-topic.topic", "my-topic-1");
        CONFLICT.put("mp.messaging.incoming.my-topic.value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        CONFLICT.put("mp.messaging.incoming.my-topic.tracing-enabled", false);

        CONFLICT.put("mp.messaging.outgoing.my-topic.connector", "smallrye-kafka");
        CONFLICT.put("mp.messaging.outgoing.my-topic.bootstrap.servers", kafka.getBootstrapServers());
        CONFLICT.put("mp.messaging.outgoing.my-topic.topic", "my-topic-1");
        CONFLICT.put("mp.messaging.outgoing.my-topic.value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        CONFLICT.put("mp.messaging.outgoing.my-topic.tracing-enabled", false);
    }

    @Test
    public void testWhenBothIncomingAndOutgoingUseTheSameName() {
        new MapBasedConfig(CONFLICT).write();
        weld.addBeanClass(Bean.class);
        assertThatThrownBy(() -> container = weld.initialize()).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class Bean {

        @Outgoing("my-topic")
        public Publisher<String> publisher() {
            return Multi.createFrom().item("0");
        }

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("my-topic")
        @Merge
        public void consumer(String input) {
            list.add(input);
        }

        public List<String> getList() {
            return list;
        }
    }

}
