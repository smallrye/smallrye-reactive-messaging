package io.smallrye.reactive.messaging;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public interface ChannelRegistry {

    Publisher<? extends Message<?>> register(String name, Publisher<? extends Message<?>> stream,
            boolean broadcast);

    Subscriber<? extends Message<?>> register(String name,
            Subscriber<? extends Message<?>> subscriber, boolean merge);

    void register(String name, Emitter<?> emitter);

    void register(String name, MutinyEmitter<?> emitter);

    void register(String name, Class<?> emitterType, Object emitter);

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
