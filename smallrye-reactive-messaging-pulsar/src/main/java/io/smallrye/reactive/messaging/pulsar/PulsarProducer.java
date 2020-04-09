package io.smallrye.reactive.messaging.pulsar;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.checkerframework.checker.units.qual.A;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.IllegalStateRuntimeException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/*
    After reading the documentation at https://www.reactive-streams.org/reactive-streams-1.0.0-javadoc/
    I came to the conclusion that the "Producer" in this case is the Pulsar Server itself.
 */

public class PulsarProducer<T extends Message> implements Publisher<Message>{

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarProducer.class);

    private final PulsarManager pulsarManager;

    private final AtomicLong requests = new AtomicLong();
    private final AtomicReference<Subscriber<? super Message>> downstream = new AtomicReference<>();
    private final ExecutorService executor;

    public PulsarProducer(Config config) {
        this.pulsarManager = new PulsarManager(config);
        this.executor = Executors.newSingleThreadExecutor();
    }


    @Override
    public void subscribe(Subscriber<? super Message> s) {
        Consumer consumer = pulsarManager.createNewConsumer();
        consumer.receiveAsync().thenAccept(message->{
                Message m = () -> {
                  return ((org.apache.pulsar.client.api.Message)message).getData();
                };
                s.onNext(m);
            }
        );
    }
}
