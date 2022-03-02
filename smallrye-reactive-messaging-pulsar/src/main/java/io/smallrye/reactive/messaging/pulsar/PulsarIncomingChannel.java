package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.Vertx;

public class PulsarIncomingChannel<T> {

    private final Consumer<T> consumer;
    private final PublisherBuilder<Message<?>> publisher;
    private final String channel;

    public PulsarIncomingChannel(PulsarClient client, Vertx vertx, Schema<T> schema,
            PulsarConnectorIncomingConfiguration ic) throws PulsarClientException {
        this.channel = ic.getChannel();
        String subscriptionName = ic.getSubscriptionName().orElseGet(() -> {
            String s = UUID.randomUUID().toString();
            log.noSubscriptionName(s);
            return s;
        });
        // TODO we need a hell of a lot more configuration here
        ConsumerBuilder<T> builder = client.newConsumer(schema)
                .consumerName(channel)
                .topic(ic.getTopic().orElse(channel))
                .subscriptionName(subscriptionName)
                //                .isAckReceiptEnabled(true) // TODO ogu add config option for this, with this ack
                //                 receipts are async, therefore acking every message and waiting for the receipt
                //                 makes the consumption is very slow.
                .subscriptionType(ic.getSubscriptionType().orElse(SubscriptionType.Exclusive))
                .acknowledgmentGroupTime(ic.getAckGroupTime(), TimeUnit.MILLISECONDS);
        if (ic.getAckTimeout().isPresent()) {
            builder.ackTimeout(ic.getAckTimeout().get(), TimeUnit.MILLISECONDS);
        }
        Optional<String> deadLetterTopic = ic.getDeadLetterPolicyDeadLetterTopic();
        Optional<String> retryLetterTopic = ic.getDeadLetterPolicyRetryLetterTopic();
        if (deadLetterTopic.isPresent() || retryLetterTopic.isPresent()) {
            Optional<Integer> deadLetterPolicyMaxRedeliverCount = ic.getDeadLetterPolicyMaxRedeliverCount();
            DeadLetterPolicy.DeadLetterPolicyBuilder deadLetterPolicyBuilder = DeadLetterPolicy.builder();
            deadLetterPolicyMaxRedeliverCount.ifPresent(deadLetterPolicyBuilder::maxRedeliverCount);
            deadLetterTopic.ifPresent(deadLetterPolicyBuilder::deadLetterTopic);
            retryLetterTopic.ifPresent(deadLetterPolicyBuilder::retryLetterTopic);
            builder.deadLetterPolicy(deadLetterPolicyBuilder.build());
        }

        this.consumer = builder
                .subscribe();
        this.publisher = ReactiveStreams.fromPublisher(Multi.createFrom().<Object, Message<?>> generator(() -> null,
                (o, emitter) -> {
                    // TODO ogu should we run this on the Vert.x thread as well ? Otherwise this will be called on request thread.
                    consumer.receiveAsync()
                            .whenComplete((m, t) -> {
                                // Callback received on pulsar-client-internal thread
                                if (t == null) {
                                    emitter.emit(
                                            new PulsarIncomingMessage<>(m, new ConsumerAcknowledgement(consumer)));
                                } else {
                                    emitter.fail(t);
                                }
                            });
                    return null;
                }).emitOn(vertx.nettyEventLoopGroup())); // TODO ogu should we create a Vert.x context for each channel ?
    }

    public PublisherBuilder<Message<?>> getPublisher() {
        return publisher;
    }

    public String getChannel() {
        return channel;
    }

    public Consumer<T> getConsumer() {
        return consumer;
    }

    public void close() {
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            log.unableToCloseConsumer(e);
        }
    }

    static class ConsumerAcknowledgement implements Acknowledgement {

        private final Consumer<?> consumer;

        ConsumerAcknowledgement(Consumer<?> consumer) {
            this.consumer = consumer;
        }

        @Override
        public CompletionStage<Void> ack(org.apache.pulsar.client.api.Message<?> message) {
            return consumer.acknowledgeAsync(message);
        }

        @Override
        public CompletionStage<Void> nack(org.apache.pulsar.client.api.Message<?> message, Throwable reason) {
            consumer.negativeAcknowledge(message);
            return CompletableFuture.completedFuture(null);
        }
    }
}
