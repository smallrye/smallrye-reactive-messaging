package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.pulsar.base.PulsarBaseTest;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class OutgoingMetadataTest extends PulsarBaseTest {

    @Test
    void testOutgoingMetadata() throws PulsarClientException {
        List<org.apache.pulsar.client.api.Message<String>> messages = new CopyOnWriteArrayList<>();

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config()
                .with("maxPendingMessagesAcrossPartitions", 10));
        PulsarOutgoingChannel<String> channel = new PulsarOutgoingChannel<>(client, Schema.STRING, oc, configResolver);

        Flow.Subscriber<? extends Message<?>> subscriber = channel.getSubscriber();

        receive(client.newConsumer(Schema.STRING)
                .consumerName("test-consumer")
                .subscriptionName("subscription")
                .topic(topic)
                .subscribe(), 5, messages::add);

        Multi.createFrom().range(0, 5)
                .map(i -> PulsarMessage.of("v-" + i, PulsarOutgoingMessageMetadata.builder()
                        .withKey("k-" + i)
                        .withDisabledReplication()
                        .withEventTime(System.currentTimeMillis())
                        .withProperties(Map.of("my-key", "my-value"))
                        .build()))
                .subscribe((Flow.Subscriber<? super Message<String>>) subscriber);

        await().until(() -> messages.size() == 5);
        assertThat(messages).allSatisfy(m -> {
            assertThat(m.hasKey()).isTrue();
            assertThat(m.getEventTime()).isNotZero();
            assertThat(m.isReplicated()).isFalse();
            assertThat(m.getProperty("my-key")).isEqualTo("my-value");
            assertThat(m.getKey()).startsWith("k-");
            assertThat(m.getTopicName()).endsWith(topic);
            assertThat(m.getProperty("my-key")).isEqualTo("my-value");
        }).extracting(org.apache.pulsar.client.api.Message::getValue)
                .containsExactly("v-" + 0, "v-" + 1, "v-" + 2, "v-" + 3, "v-" + 4);
    }

    @Test
    void testOutgoingMessage() throws PulsarClientException {
        List<org.apache.pulsar.client.api.Message<String>> messages = new CopyOnWriteArrayList<>();

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config()
                .with("maxPendingMessagesAcrossPartitions", 10));
        PulsarOutgoingChannel<String> channel = new PulsarOutgoingChannel<>(client, Schema.STRING, oc, configResolver);

        Flow.Subscriber<? extends Message<?>> subscriber = channel.getSubscriber();

        receive(client.newConsumer(Schema.STRING)
                .consumerName("test-consumer")
                .subscriptionName("subscription")
                .topic(topic)
                .subscribe(), 5, messages::add);

        Multi.createFrom().range(0, 5)
                .map(i -> new OutgoingMessage<>("v-" + i)
                        .withKey("k-" + i)
                        .withSequenceId(i)
                        .withDisabledReplication()
                        .withEventTime(System.currentTimeMillis())
                        .withProperties(Map.of("my-key", "my-value")))
                .map(Message::of)
                .subscribe((Flow.Subscriber<? super Message<OutgoingMessage<String>>>) subscriber);

        await().until(() -> messages.size() == 5);
        assertThat(messages).allSatisfy(m -> {
            assertThat(m.hasKey()).isTrue();
            assertThat(m.getEventTime()).isNotZero();
            assertThat(m.isReplicated()).isFalse();
            assertThat(m.getProperty("my-key")).isEqualTo("my-value");
            assertThat(m.getKey()).startsWith("k-");
            assertThat(m.getTopicName()).endsWith(topic);
            assertThat(m.getProperty("my-key")).isEqualTo("my-value");
        }).extracting(org.apache.pulsar.client.api.Message::getValue)
                .containsExactly("v-" + 0, "v-" + 1, "v-" + 2, "v-" + 3, "v-" + 4);
    }

    MapBasedConfig config() {
        return baseConfig()
                .with("channel-name", "channel")
                .with("topic", topic);
    }
}
