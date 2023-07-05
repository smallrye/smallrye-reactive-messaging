package io.smallrye.reactive.messaging.providers.impl;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MutinyEmitter;

@ApplicationScoped
public class InternalChannelRegistry implements ChannelRegistry {

    private final Map<String, List<Flow.Publisher<? extends Message<?>>>> publishers = new ConcurrentHashMap<>();
    private final Map<String, List<Flow.Subscriber<? extends Message<?>>>> subscribers = new ConcurrentHashMap<>();

    private final Map<String, Boolean> outgoing = new ConcurrentHashMap<>();
    private final Map<String, Boolean> incoming = new ConcurrentHashMap<>();

    private final Map<Class<?>, Map<String, Object>> emitters = new ConcurrentHashMap<>();

    @Override
    public Flow.Publisher<? extends Message<?>> register(String name,
            Flow.Publisher<? extends Message<?>> stream, boolean broadcast) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(stream, msg.streamMustBeSet());
        register(publishers, name, stream);
        outgoing.put(name, broadcast);
        return stream;
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> register(String name,
            Flow.Subscriber<? extends Message<?>> subscriber, boolean merge) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(subscriber, msg.subscriberMustBeSet());
        register(subscribers, name, subscriber);
        incoming.put(name, merge);
        return subscriber;
    }

    @Override
    public void register(String name, Emitter<?> emitter) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(emitter, msg.emitterMustBeSet());
        register(name, Emitter.class, emitter);
    }

    @Override
    public void register(String name, MutinyEmitter<?> emitter) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(emitter, msg.emitterMustBeSet());
        register(name, MutinyEmitter.class, emitter);
    }

    @Override
    public <T> void register(String name, Class<T> emitterType, T emitter) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(emitter, msg.emitterMustBeSet());
        Map<String, Object> map = emitters.computeIfAbsent(emitterType, key -> new ConcurrentHashMap<>());
        map.put(name, emitter);
    }

    @Override
    public List<Flow.Publisher<? extends Message<?>>> getPublishers(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return publishers.getOrDefault(name, Collections.emptyList());
    }

    @Override
    public Emitter<?> getEmitter(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return getEmitter(name, Emitter.class);
    }

    @Override
    public MutinyEmitter<?> getMutinyEmitter(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return getEmitter(name, MutinyEmitter.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getEmitter(String name, Class<? super T> emitterType) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Map<String, Object> typedEmitters = emitters.get(emitterType);
        if (typedEmitters == null) {
            return null;
        } else {
            return (T) typedEmitters.get(name);
        }
    }

    @Override
    public List<Flow.Subscriber<? extends Message<?>>> getSubscribers(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return subscribers.getOrDefault(name, Collections.emptyList());
    }

    private <T> void register(Map<String, List<T>> multimap, String name, T item) {
        List<T> list = multimap.computeIfAbsent(name, key -> new CopyOnWriteArrayList<>());
        list.add(item);
    }

    @Override
    public Set<String> getIncomingNames() {
        return publishers.keySet();
    }

    @Override
    public Set<String> getOutgoingNames() {
        return subscribers.keySet();
    }

    @Override
    public Set<String> getEmitterNames() {
        return emitters.values().stream().flatMap(m -> m.keySet().stream()).collect(Collectors.toSet());
    }

    @Override
    public Map<String, Boolean> getIncomingChannels() {
        return outgoing;
    }

    @Override
    public Map<String, Boolean> getOutgoingChannels() {
        return incoming;
    }

}
