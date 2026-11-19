package io.smallrye.reactive.messaging.providers.impl;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.PausableChannel;

@ApplicationScoped
public class InternalChannelRegistry implements ChannelRegistry {

    private final Map<String, List<Flow.Publisher<? extends Message<?>>>> publishers = new ConcurrentHashMap<>();
    private final Map<String, List<Flow.Subscriber<? extends Message<?>>>> subscribers = new ConcurrentHashMap<>();

    private final Map<String, Boolean> outgoing = new ConcurrentHashMap<>();
    private final Map<String, Boolean> incoming = new ConcurrentHashMap<>();

    private final Map<Class<?>, Map<String, MessagePublisherProvider<?>>> emitters = new ConcurrentHashMap<>();
    private final Map<String, PausableChannel> pausables = new ConcurrentHashMap<>();
    private final Map<String, String> incomingConnectors = new ConcurrentHashMap<>();
    private final Map<String, String> outgoingConnectors = new ConcurrentHashMap<>();
    private final Map<String, List<String>> incomingConnectorChannels = new ConcurrentHashMap<>();

    @Override
    public Flow.Publisher<? extends Message<?>> register(String name,
            Flow.Publisher<? extends Message<?>> stream, boolean broadcast) {
        return register(name, null, stream, broadcast);
    }

    @Override
    public Flow.Publisher<? extends Message<?>> register(String name, String connector,
            Flow.Publisher<? extends Message<?>> stream, boolean broadcast) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(stream, msg.streamMustBeSet());
        register(publishers, name, stream);
        outgoing.put(name, broadcast);
        if (connector != null) {
            incomingConnectors.put(name, connector);
        }
        return stream;
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> register(String name,
            Flow.Subscriber<? extends Message<?>> subscriber, boolean merge) {
        return register(name, null, subscriber, merge);
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> register(String name, String connector,
            Flow.Subscriber<? extends Message<?>> subscriber, boolean merge) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(subscriber, msg.subscriberMustBeSet());
        register(subscribers, name, subscriber);
        incoming.put(name, merge);
        if (connector != null) {
            outgoingConnectors.put(name, connector);
        }
        return subscriber;
    }

    @Override
    public void register(String name, Emitter<?> emitter) {
        register(name, (String) null, emitter);
    }

    @Override
    public void register(String name, String connector, Emitter<?> emitter) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(emitter, msg.emitterMustBeSet());
        register(name, connector, Emitter.class, emitter);
    }

    @Override
    public void register(String name, MutinyEmitter<?> emitter) {
        register(name, (String) null, emitter);
    }

    @Override
    public void register(String name, String connector, MutinyEmitter<?> emitter) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(emitter, msg.emitterMustBeSet());
        register(name, connector, MutinyEmitter.class, emitter);
    }

    @Override
    public <T> void register(String name, Class<T> emitterType, T emitter) {
        register(name, null, emitterType, emitter);
    }

    @Override
    public <T> void register(String name, String connector, Class<T> emitterType, T emitter) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        Objects.requireNonNull(emitter, msg.emitterMustBeSet());
        emitters.computeIfAbsent(emitterType, key -> new ConcurrentHashMap<>())
                .put(name, (MessagePublisherProvider<?>) emitter);
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
        Map<String, MessagePublisherProvider<?>> typedEmitters = emitters.get(emitterType);
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
    public List<MessagePublisherProvider<?>> getEmitters(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return emitters.values().stream()
                .map(m -> m.get(name))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, Boolean> getIncomingChannels() {
        return outgoing;
    }

    @Override
    public Map<String, Boolean> getOutgoingChannels() {
        return incoming;
    }

    @Override
    public void register(String name, PausableChannel pausable) {
        pausables.put(name, pausable);
    }

    @Override
    public PausableChannel getPausable(String name) {
        return pausables.get(name);
    }

    @Override
    public Map<String, PausableChannel> getPausableChannels() {
        return Collections.unmodifiableMap(pausables);
    }

    @Override
    public String getIncomingConnectorName(String channel) {
        return incomingConnectors.get(channel);
    }

    @Override
    public String getOutgoingConnectorName(String channel) {
        return outgoingConnectors.get(channel);
    }

    /**
     * Records the names under which the connector knows the parts of an incoming channel. A channel configured with
     * {@code concurrency} is consumed through several indexed connector channels ({@code channel$1}, {@code channel$2},
     * ...), while it is registered under its base name.
     *
     * @param channel the channel name, as registered
     * @param connectorChannels the channel names the connector uses
     */
    public void registerIncomingConnectorChannels(String channel, List<String> connectorChannels) {
        Objects.requireNonNull(channel, msg.nameMustBeSet());
        incomingConnectorChannels.put(channel, List.copyOf(connectorChannels));
    }

    /**
     * @param channel the channel name, as registered
     * @return the channel names the connector uses for this incoming channel: the indexed channels for a channel
     *         configured with {@code concurrency}, the channel itself otherwise
     */
    public List<String> getIncomingConnectorChannels(String channel) {
        Objects.requireNonNull(channel, msg.nameMustBeSet());
        return incomingConnectorChannels.getOrDefault(channel, List.of(channel));
    }

    /**
     * @param name the channel name
     * @return {@code true} if a publisher, a subscriber or an emitter is registered under this name
     */
    public boolean isRegistered(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        return publishers.containsKey(name) || subscribers.containsKey(name)
                || emitters.values().stream().anyMatch(m -> m.containsKey(name));
    }

    /**
     * Removes an incoming channel: its publishers, broadcast flag, connector name, emitters and pausable channel,
     * including the indexed connector channels of a channel configured with {@code concurrency}.
     *
     * @param name the channel name, as registered
     */
    public void unregisterIncoming(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        List<String> connectorChannels = incomingConnectorChannels.remove(name);
        if (connectorChannels != null) {
            connectorChannels.forEach(pausables::remove);
        }
        publishers.remove(name);
        outgoing.remove(name);
        incomingConnectors.remove(name);
        emitters.values().forEach(m -> m.remove(name));
        pausables.remove(name);
    }

    /**
     * Removes an outgoing channel: its subscribers, merge flag, connector name, emitters and pausable channel.
     * <p>
     * An outgoing channel fed by an emitter or a source also registers its upstream publisher under the same name:
     * it is removed too, unless the channel is also known as an incoming connector channel.
     *
     * @param name the channel name, as registered
     */
    public void unregisterOutgoing(String name) {
        Objects.requireNonNull(name, msg.nameMustBeSet());
        subscribers.remove(name);
        incoming.remove(name);
        outgoingConnectors.remove(name);
        emitters.values().forEach(m -> m.remove(name));
        if (!incomingConnectors.containsKey(name)) {
            publishers.remove(name);
            outgoing.remove(name);
            pausables.remove(name);
        }
    }

    @Override
    public Map<String, String> getConnectorNames() {
        Map<String, String> all = new ConcurrentHashMap<>(incomingConnectors);
        all.putAll(outgoingConnectors);
        return Collections.unmodifiableMap(all);
    }

}
