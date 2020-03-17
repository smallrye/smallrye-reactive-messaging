package io.smallrye.reactive.messaging.gcp.pubsub;

import java.util.Objects;
import java.util.function.Consumer;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.subscription.MultiEmitter;

public class PubSubSource implements Consumer<MultiEmitter<? super Message<?>>> {

    private final PubSubConfig config;

    private final PubSubManager manager;

    public PubSubSource(final PubSubConfig config, final PubSubManager manager) {
        this.config = Objects.requireNonNull(config, "config is required");
        this.manager = Objects.requireNonNull(manager, "manager is required");
    }

    @Override
    public void accept(MultiEmitter<? super Message<?>> emitter) {
        manager.subscriber(config, emitter);
    }

}
