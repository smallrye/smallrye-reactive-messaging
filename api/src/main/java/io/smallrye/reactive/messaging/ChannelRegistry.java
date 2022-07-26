package io.smallrye.reactive.messaging;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

public interface ChannelRegistry {

    Publisher<? extends Message<?>> register(String name, Publisher<? extends Message<?>> stream,
            boolean broadcast);

    Subscriber<? extends Message<?>> register(String name,
            Subscriber<? extends Message<?>> subscriber, boolean merge);

    void register(String name, Emitter<?> emitter);

    void register(String name, MutinyEmitter<?> emitter);

    <T> void register(String name, Class<T> emitterType, T emitter);

    List<Publisher<? extends Message<?>>> getPublishers(String name);

    Emitter<?> getEmitter(String name);

    MutinyEmitter<?> getMutinyEmitter(String name);

    <T> T getEmitter(String name, Class<? super T> emitterType);

    List<Subscriber<? extends Message<?>>> getSubscribers(String name);

    Set<String> getIncomingNames();

    Set<String> getOutgoingNames();

    Set<String> getEmitterNames();

    Map<String, Boolean> getIncomingChannels();

    Map<String, Boolean> getOutgoingChannels();

}
