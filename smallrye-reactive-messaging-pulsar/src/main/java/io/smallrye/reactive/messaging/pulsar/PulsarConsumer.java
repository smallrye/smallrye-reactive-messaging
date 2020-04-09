package io.smallrye.reactive.messaging.pulsar;

import io.reactivex.Completable;
import org.apache.pulsar.client.api.Consumer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/*
    After reading the documentation at https://www.reactive-streams.org/reactive-streams-1.0.0-javadoc/
    I came to the conclusion that the "Consumer" in this case is the Pulsar Server itself.
 */
public class PulsarConsumer<T extends Message> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConsumer.class);
    private final PulsarManager pulsarManager;
    private final ExecutorService executor;
    private final Consumer pulsarConsumer;
    private org.apache.pulsar.client.api.Message message;

    public PulsarConsumer(Config config) {
        this.pulsarManager = new PulsarManager(config);
        this.pulsarConsumer = pulsarManager.createNewConsumer();
        this.executor = Executors.newSingleThreadExecutor();
    }

    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder() {
        return ReactiveStreams.<Message<?>> builder()
            .flatMapCompletionStage(message -> {
                return pulsarConsumer.receiveAsync();
            }).onError(t -> LOGGER.error("Unable to dispatch message to Kafka", t))
            .ignore();
    }


    public void close() {
        pulsarConsumer.closeAsync();
    }

}
