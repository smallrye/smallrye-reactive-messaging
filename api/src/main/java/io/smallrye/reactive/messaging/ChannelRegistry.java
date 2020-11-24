package io.smallrye.reactive.messaging;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

public interface ChannelRegistry {

    default PublisherBuilder<? extends Message<?>> register(String name, PublisherBuilder<? extends Message<?>> stream) {
        return register(name, stream, false);
    }

    PublisherBuilder<? extends Message<?>> register(String name, PublisherBuilder<? extends Message<?>> stream,
            boolean broadcast);

    SubscriberBuilder<? extends Message<?>, Void> register(String name,
            SubscriberBuilder<? extends Message<?>, Void> subscriber);

    void register(String name, Emitter<?> emitter);

    void register(String name, MutinyEmitter<?> emitter);

    List<PublisherBuilder<? extends Message<?>>> getPublishers(String name);

    Emitter<?> getEmitter(String name);

    MutinyEmitter<?> getMutinyEmitter(String name);

    List<SubscriberBuilder<? extends Message<?>, Void>> getSubscribers(String name);

    Set<String> getIncomingNames();

    Set<String> getOutgoingNames();

    Set<String> getEmitterNames();

    Map<String, Boolean> getIncomingChannels();
}
