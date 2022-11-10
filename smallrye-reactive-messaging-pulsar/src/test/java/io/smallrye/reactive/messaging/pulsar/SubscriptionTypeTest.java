package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.ack.PulsarMessageAck;
import io.smallrye.reactive.messaging.pulsar.base.PulsarBaseTest;
import io.smallrye.reactive.messaging.pulsar.fault.PulsarNack;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SubscriptionTypeTest extends PulsarBaseTest {

    private static final int NUMBER_OF_MESSAGES = 10;

    @Test
    void testExclusiveSubscription() throws PulsarClientException {
        List<Message<?>> messages1 = new CopyOnWriteArrayList<>();
        List<Message<?>> messages2 = new CopyOnWriteArrayList<>();

        String subscriptionName = "exclusive-subscription";
        PulsarIncomingChannel<Integer> channel = createChannel(config()
                .with("subscriptionType", "Exclusive")
                .with("subscriptionName", subscriptionName));
        Multi.createFrom().publisher(channel.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages1::add);

        assertThatThrownBy(() -> {
            PulsarIncomingChannel<Integer> channel2 = createChannel(config()
                    .with("subscriptionType", "Exclusive")
                    .with("subscriptionName", subscriptionName));
            Multi.createFrom().publisher(channel2.getPublisher())
                    .subscribe().with(messages2::add);
        }).isInstanceOf(PulsarClientException.ConsumerBusyException.class);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i).value(i));

        await().until(() -> messages1.size() == NUMBER_OF_MESSAGES);
        assertThat(messages1).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> (int) m.getPayload()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, NUMBER_OF_MESSAGES).boxed().collect(Collectors.toList()));

        assertThat(messages2).isEmpty();
    }

    @Test
    void testExclusiveSubscriptionUniqueSubscriptionName() throws PulsarClientException {
        List<Message<?>> messages1 = new CopyOnWriteArrayList<>();
        List<Message<?>> messages2 = new CopyOnWriteArrayList<>();

        PulsarIncomingChannel<Integer> channel = createChannel(config()
                .with("subscriptionType", "Exclusive")
                .with("subscriptionName", UUID.randomUUID().toString()));
        Multi.createFrom().publisher(channel.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages1::add);

        PulsarIncomingChannel<Integer> channel2 = createChannel(config()
                .with("subscriptionType", "Exclusive")
                .with("subscriptionName", UUID.randomUUID().toString()));
        Multi.createFrom().publisher(channel2.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages2::add);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .accessMode(ProducerAccessMode.Shared)
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i).value(i));

        await().until(() -> messages1.size() == NUMBER_OF_MESSAGES);
        assertThat(messages1).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> (int) m.getPayload()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, NUMBER_OF_MESSAGES).boxed().collect(Collectors.toList()));

        await().until(() -> messages2.size() == NUMBER_OF_MESSAGES);
        assertThat(messages1).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> (int) m.getPayload()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, NUMBER_OF_MESSAGES).boxed().collect(Collectors.toList()));

    }

    @Test
    void testFailoverSubscription() throws PulsarClientException {
        List<Message<?>> messages1 = new CopyOnWriteArrayList<>();
        List<Message<?>> messages2 = new CopyOnWriteArrayList<>();

        String subscriptionName = "failover-subscription";
        PulsarIncomingChannel<Integer> channel = createChannel(config()
                .with("subscriptionType", "Failover")
                .with("consumerName", topic + "-consumer")
                .with("subscriptionName", subscriptionName));
        Multi.createFrom().publisher(channel.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages1::add);

        PulsarIncomingChannel<Integer> channel2 = createChannel(config()
                .with("subscriptionType", "Failover")
                .with("consumerName", topic + "-consumer-2")
                .with("subscriptionName", subscriptionName));
        Multi.createFrom().publisher(channel2.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages2::add);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i).value(i));

        await().untilAsserted(() -> assertThat(messages1.size() + messages2.size()).isEqualTo(NUMBER_OF_MESSAGES));
        List<Message<?>> allMessages = new ArrayList<>(messages1);
        allMessages.addAll(messages2);

        assertThat(allMessages).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> (int) m.getPayload()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, NUMBER_OF_MESSAGES).boxed().collect(Collectors.toList()));

        channel.close();

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer-2")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + NUMBER_OF_MESSAGES + i).value(NUMBER_OF_MESSAGES + i));

        await().untilAsserted(() -> assertThat(messages1.size() + messages2.size()).isEqualTo(NUMBER_OF_MESSAGES * 2));

        assertThat(messages2).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> (int) m.getPayload()).containsSequence(
                IntStream.range(NUMBER_OF_MESSAGES, NUMBER_OF_MESSAGES * 2).boxed().collect(Collectors.toList()));
    }

    @Test
    void testFailoverSubscriptionPartitionedTopic() throws PulsarClientException, PulsarAdminException {
        admin.topics().createPartitionedTopic(topic, 3);

        List<Message<?>> messages1 = new CopyOnWriteArrayList<>();
        List<Message<?>> messages2 = new CopyOnWriteArrayList<>();

        String subscriptionName = "failover-subscription";
        PulsarIncomingChannel<Integer> channel = createChannel(config()
                .with("subscriptionType", "Failover")
                .with("consumerName", topic + "-consumer")
                .with("subscriptionName", subscriptionName));
        Multi.createFrom().publisher(channel.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages1::add);

        PulsarIncomingChannel<Integer> channel2 = createChannel(config()
                .with("subscriptionType", "Failover")
                .with("consumerName", topic + "-consumer-2")
                .with("subscriptionName", subscriptionName));
        Multi.createFrom().publisher(channel2.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages2::add);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i).value(i));

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer-2")
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + NUMBER_OF_MESSAGES + i).value(NUMBER_OF_MESSAGES + i));

        await().until(() -> messages1.size() > 0 && messages2.size() > 0
                && (messages1.size() + messages2.size() == NUMBER_OF_MESSAGES * 2));

        List<Message<?>> allMessages = Stream.concat(messages1.stream(), messages2.stream()).collect(Collectors.toList());
        assertThat(allMessages).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> (int) m.getPayload()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, NUMBER_OF_MESSAGES * 2).boxed().collect(Collectors.toList()));
    }

    @Test
    void testSharedSubscription() throws PulsarClientException {
        List<Message<?>> messages1 = new CopyOnWriteArrayList<>();
        List<Message<?>> messages2 = new CopyOnWriteArrayList<>();

        String subscriptionName = "shared-subscription";
        PulsarIncomingChannel<Integer> channel = createChannel(config()
                .with("subscriptionType", "Shared")
                .with("consumerName", topic + "-consumer")
                .with("subscriptionName", subscriptionName));
        Multi.createFrom().publisher(channel.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages1::add);

        PulsarIncomingChannel<Integer> channel2 = createChannel(config()
                .with("subscriptionType", "Shared")
                .with("consumerName", topic + "-consumer-2")
                .with("subscriptionName", subscriptionName));
        Multi.createFrom().publisher(channel2.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages2::add);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i).value(i));

        await().until(() -> messages1.size() > 0 && messages2.size() > 0
                && (messages1.size() + messages2.size() == NUMBER_OF_MESSAGES));

        List<Message<?>> allMessages = Stream.concat(messages1.stream(), messages2.stream()).collect(Collectors.toList());
        assertThat(allMessages).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> (int) m.getPayload()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, NUMBER_OF_MESSAGES).boxed().collect(Collectors.toList()));
    }

    @Test
    void testSharedSubscriptionFailedConsumer() throws PulsarClientException {
        List<Message<?>> messages1 = new CopyOnWriteArrayList<>();
        List<Message<?>> messages2 = new CopyOnWriteArrayList<>();

        String subscriptionName = "shared-subscription-failed-consumer";
        PulsarIncomingChannel<Integer> channel = createChannel(config()
                .with("subscriptionType", "Shared")
                .with("consumerName", topic + "-consumer")
                .with("subscriptionName", subscriptionName));
        Multi.createFrom().publisher(channel.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.nack(new IllegalArgumentException())))
                .subscribe().with(messages1::add);

        PulsarIncomingChannel<Integer> channel2 = createChannel(config()
                .with("subscriptionType", "Shared")
                .with("consumerName", topic + "-consumer-2")
                .with("subscriptionName", subscriptionName));
        Multi.createFrom().publisher(channel2.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages2::add);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i).value(i));

        channel.close();

        await().until(() -> messages2.size() == NUMBER_OF_MESSAGES);

        assertThat(messages2).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> (int) m.getPayload()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, NUMBER_OF_MESSAGES).boxed().collect(Collectors.toList()));
    }

    @Test
    void testKeySharedSubscription() throws PulsarClientException {
        List<Message<?>> messages1 = new CopyOnWriteArrayList<>();
        List<Message<?>> messages2 = new CopyOnWriteArrayList<>();

        String subscriptionName = "shared-subscription-failed-consumer";
        PulsarIncomingChannel<Integer> channel = createChannel(config()
                .with("subscriptionType", "Key_Shared")
                .with("consumerName", topic + "-consumer")
                .with("subscriptionName", subscriptionName));

        Multi.createFrom().publisher(channel.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages1::add);

        PulsarIncomingChannel<Integer> channel2 = createChannel(config()
                .with("subscriptionType", "Key_Shared")
                .with("consumerName", topic + "-consumer-2")
                .with("subscriptionName", subscriptionName));

        Multi.createFrom().publisher(channel2.getPublisher())
                .onItem().call(m -> Uni.createFrom().completionStage(m.ack()))
                .subscribe().with(messages2::add);

        send(client.newProducer(Schema.INT32)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("k-" + i % 2).value(i));

        await().until(() -> messages1.size() + messages2.size() == NUMBER_OF_MESSAGES);

        List<Message<?>> allMessages = Stream.concat(messages1.stream(), messages2.stream()).collect(Collectors.toList());
        assertThat(allMessages).allSatisfy(m -> {
            assertThat(m).isInstanceOf(PulsarIncomingMessage.class);
            assertThat(m.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        }).extracting(m -> (int) m.getPayload()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, NUMBER_OF_MESSAGES).boxed().collect(Collectors.toList()));
    }

    @NotNull
    private PulsarIncomingChannel<Integer> createChannel(MapBasedConfig config) throws PulsarClientException {
        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(config);
        return new PulsarIncomingChannel<>(client, vertx, Schema.INT32,
                new PulsarMessageAck.Factory(), new PulsarNack.Factory(), ic, configResolver);
    }

    MapBasedConfig config() {
        return baseConfig()
                .with("channel-name", "channel")
                .with("subscriptionInitialPosition", "Earliest")
                .with("topic", topic);
    }
}
