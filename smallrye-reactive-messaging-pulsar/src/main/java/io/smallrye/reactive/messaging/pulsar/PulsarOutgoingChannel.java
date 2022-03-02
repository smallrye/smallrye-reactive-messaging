package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.mutiny.Uni;

public class PulsarOutgoingChannel<T> {

    private final Producer<T> producer;
    private final PulsarSenderProcessor processor;
    private final SubscriberBuilder<Message<?>, Void> subscriber;
    private final String channel;

    public PulsarOutgoingChannel(PulsarClient client, Schema<T> schema,
            PulsarConnectorOutgoingConfiguration oc) throws PulsarClientException {
        this.channel = oc.getChannel();
        // TODO we need a hell of a lot more configuration here
        this.producer = client.newProducer(schema)
                .topic(oc.getTopic().orElse(oc.getChannel()))
                .blockIfQueueFull(true)
                .maxPendingMessages(oc.getMaxPendingMessages()) // TODO: do we really want this setting? this affects how sendAsync behaves
                .create();
        long requests = oc.getMaxPendingMessages();
        if (requests <= 0) {
            // TODO: should this be disallowed?
            requests = Long.MAX_VALUE;
        }

        processor = new PulsarSenderProcessor(requests, true, this::sendMessage);
        subscriber = ReactiveStreams.<Message<?>> builder()
                .via(processor)
                .onError(log::unableToDispatch)
                .ignore();
    }

    private Uni<Void> sendMessage(Message<?> message) {
        TypedMessageBuilder<?> messageBuilder = producer.newMessage()
                // TODO: we need some intelligence regarding the payload type
                .value((T) message.getPayload())
        /*
         * TODO ogu transform some outgoing metadata to key ordering key etc. etc.
         * .key()
         * .keyBytes()
         * .orderingKey()
         * .disableReplication()
         * .eventTime()
         * .properties()
         * .sequenceId()
         * .replicationClusters()
         */;
        // TODO Furthermore, we need to figure out if sendAsync should block when the queue is full, or throw an exception
        // TODO ogu : with PulsarSenderProcessor configured for maximum maxPendingMessages requsts we should not block/throw. I went for block=true
        return Uni.createFrom().completionStage(messageBuilder.sendAsync())
                .onItemOrFailure().transformToUni((mid, t) -> {
                    if (t == null) {
                        return Uni.createFrom().completionStage(message.ack());
                    } else {
                        return Uni.createFrom().completionStage(message.nack(t));
                    }
                });
        // TODO: what thread is the subscription supposed to run on?
    }

    public SubscriberBuilder<Message<?>, Void> getSubscriber() {
        return subscriber;
    }

    public String getChannel() {
        return channel;
    }

    public Producer<T> getProducer() {
        return producer;
    }

    public void close() {
        if (processor != null) {
            processor.cancel();
        }
        try {
            producer.close();
        } catch (PulsarClientException e) {
            log.unableToCloseProducer(e);
        }
    }
}
