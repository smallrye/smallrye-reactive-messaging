package io.smallrye.reactive.messaging.gcp.pubsub;

import java.util.Objects;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;

public class PubSubFlowableOnSubscribe implements FlowableOnSubscribe<Message<?>> {

    private final PubSubConfig config;

    private final PubSubManager manager;

    public PubSubFlowableOnSubscribe(final PubSubConfig config, final PubSubManager manager) {
        this.config = Objects.requireNonNull(config, "config is required");
        this.manager = Objects.requireNonNull(manager, "manager is required");
    }

    @Override
    public void subscribe(final FlowableEmitter<Message<?>> emitter) {
        // emitter.onNext may be called from multiple threads. use a
        // serialized emitter here to avoid undefined behavior.
        manager.subscriber(config, emitter.serialize());
    }

}
