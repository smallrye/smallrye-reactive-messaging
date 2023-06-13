package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.pulsar.ack.PulsarMessageAck;
import io.smallrye.reactive.messaging.pulsar.base.PulsarBaseTest;
import io.smallrye.reactive.messaging.pulsar.fault.PulsarNack;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class IncomingMetadataTest extends PulsarBaseTest {

    @Test
    void testIncomingMetadata() throws PulsarClientException {
        List<Message<?>> messages = new CopyOnWriteArrayList<>();

        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(config());
        PulsarIncomingChannel<Integer> channel = new PulsarIncomingChannel<>(client, vertx, Schema.INT32,
                new PulsarMessageAck.Factory(), new PulsarNack.Factory(),
                ic, configResolver);
        Multi.createFrom().publisher(channel.getPublisher())
                .subscribe().with(messages::add);

        send(client.newProducer(Schema.INT32).topic(topic).create(), 5, (i, p) -> p.newMessage()
                .key("k-" + i)
                .value(i)
                .property("my-key", "my-value"));

        await().until(() -> messages.size() >= 5);

        assertThat(messages).allSatisfy(m -> {
            PulsarIncomingMessage<?> pulsarMsg = m.unwrap(PulsarIncomingMessage.class);
            assertThat(pulsarMsg.unwrap()).isNotNull();
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
            PulsarIncomingMessageMetadata metadata = m.getMetadata(PulsarIncomingMessageMetadata.class).get();
            assertThat(metadata.hasKey()).isTrue();
            assertThat(metadata.getKey()).startsWith("k-");
            assertThat(metadata.getTopicName()).endsWith(topic);
            assertThat(metadata.getProperty("my-key")).isEqualTo("my-value");
        }).extracting(m -> (int) m.getPayload())
                .containsExactly(0, 1, 2, 3, 4);
    }

    MapBasedConfig config() {
        return baseConfig()
                .with("channel-name", "channel")
                .with("topic", topic);
    }
}
