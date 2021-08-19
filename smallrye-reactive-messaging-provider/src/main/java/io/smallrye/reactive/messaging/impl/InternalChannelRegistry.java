package io.smallrye.reactive.messaging.impl;

import static io.smallrye.reactive.messaging.i18n.ProviderMessages.msg;

import java.util.*;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MutinyEmitter;

@ApplicationScoped
public class InternalChannelRegistry implements ChannelRegistry {

    private final Map<String, List<Publisher<? extends Message<?>>>> publishers = new HashMap<>();
    private final Map<String, List<Subscriber<? extends Message<?>>>> subscribers = new HashMap<>();

    private final Map<String, Boolean> outgoing = new HashMap<>();
    private final Map<String, Boolean> incoming = new HashMap<>();

    private final Map<String, Emitter<?>> emitters = new HashMap<>();
    private final Map<String, MutinyEmitter<?>> mutinyEmitters = new HashMap<>();


    @Override
    public Publisher<? extends Message<?>> register(String name,
            Publisher<? extends Message<?>> stream, boolean broadcast) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(stream, msg.streamMustBeSet());
        register(publishers, name, stream);
        outgoing.put(name, broadcast);
        return stream;
    }

    @Override
    public synchronized Subscriber<? extends Message<?>> register(String name,
            Subscriber<? extends Message<?>> subscriber, boolean merge) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(subscriber, msg.subscriberMustBeSet());
        register(subscribers, name, subscriber);
        incoming.put(name, merge);
        return subscriber;
    }

    @Override
    public synchronized void register(String name, Emitter<?> emitter) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(emitter, msg.emitterMustBeSet());
        emitters.put(name, emitter);
    }

    @Override
    public synchronized void register(String name, MutinyEmitter<?> emitter) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(emitter, msg.emitterMustBeSet());
        mutinyEmitters.put(name, emitter);
    }

    @Override
    public synchronized List<Publisher<? extends Message<?>>> getPublishers(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return publishers.getOrDefault(name, Collections.emptyList());
    }

    @Override
    public synchronized Emitter<?> getEmitter(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return emitters.get(name);
    }

    @Override
    public synchronized MutinyEmitter<?> getMutinyEmitter(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return mutinyEmitters.get(name);
    }

    @Override
    public synchronized List<Subscriber<? extends Message<?>>> getSubscribers(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return subscribers.getOrDefault(name, Collections.emptyList());
    }

    private <T> void register(Map<String, List<T>> multimap, String name, T item) {
        List<T> list = multimap.computeIfAbsent(name, key -> new ArrayList<>());
        list.add(item);
    }

    @Override
    public synchronized Set<String> getIncomingNames() {
        return new HashSet<>(publishers.keySet());
    }

    @Override
    public synchronized Set<String> getOutgoingNames() {
        return new HashSet<>(subscribers.keySet());
    }

    @Override
    public synchronized Set<String> getEmitterNames() {
        Set<String> set = new HashSet<>();
        set.addAll(emitters.keySet());
        set.addAll(mutinyEmitters.keySet());
        return set;
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
