package io.smallrye.reactive.messaging.providers.connectors;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;

@ApplicationScoped
@Connector("smallrye-dummy")
public class MyDummyConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {
    private final List<String> list = new CopyOnWriteArrayList<>();
    private final List<Config> configs = new CopyOnWriteArrayList<>();
    private boolean completed = false;

    public void reset() {
        list.clear();
        completed = false;
    }

    public List<String> list() {
        return list;
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        this.configs.add(config);
        return ReactiveStreams.<Message<?>> builder()
                .peek(x -> list.add(x.getPayload().toString()))
                .peek(x -> x.getMetadata(OutgoingMessageMetadata.class).ifPresent(m -> m.setResult(x.getPayload())))
                .peek(x -> {
                    try {
                        x.ack().toCompletableFuture().get();
                    } catch (InterruptedException | ExecutionException e) {
                        try {
                            x.nack(e).toCompletableFuture().get();
                        } catch (InterruptedException | ExecutionException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                })
                .onComplete(() -> completed = true)
                .ignore();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        this.configs.add(config);
        int increment = config.getOptionalValue("increment", Integer.class).orElse(1);
        return ReactiveStreams
                .fromPublisher(Flowable.just(1, 2, 3).map(i -> i + increment).map(Message::of));
    }

    boolean gotCompletion() {
        return completed;
    }

    List<Config> getConfigs() {
        return configs;
    }
}
