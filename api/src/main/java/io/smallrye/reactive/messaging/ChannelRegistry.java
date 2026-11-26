package io.smallrye.reactive.messaging;

import java.util.ArrayList;
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

    /**
     * Registers an incoming channel stream together with the name of the connector it comes from.
     * <p>
     * The default implementation ignores the connector name and delegates to
     * {@link #register(String, Publisher, boolean)}.
     */
    default Publisher<? extends Message<?>> register(String name, String connector, Publisher<? extends Message<?>> stream,
            boolean broadcast) {
        return register(name, stream, broadcast);
    }

    Subscriber<? extends Message<?>> register(String name,
            Subscriber<? extends Message<?>> subscriber, boolean merge);

    /**
     * Registers an outgoing channel subscriber together with the name of the connector it goes to.
     * <p>
     * The default implementation ignores the connector name and delegates to
     * {@link #register(String, Subscriber, boolean)}.
     */
    default Subscriber<? extends Message<?>> register(String name, String connector,
            Subscriber<? extends Message<?>> subscriber, boolean merge) {
        return register(name, subscriber, merge);
    }

    void register(String name, Emitter<?> emitter);

    /**
     * Registers an emitter together with the name of the connector its channel goes to.
     * <p>
     * The default implementation ignores the connector name and delegates to {@link #register(String, Emitter)}.
     */
    default void register(String name, String connector, Emitter<?> emitter) {
        register(name, emitter);
    }

    void register(String name, MutinyEmitter<?> emitter);

    /**
     * Registers a Mutiny emitter together with the name of the connector its channel goes to.
     * <p>
     * The default implementation ignores the connector name and delegates to
     * {@link #register(String, MutinyEmitter)}.
     */
    default void register(String name, String connector, MutinyEmitter<?> emitter) {
        register(name, emitter);
    }

    <T> void register(String name, Class<T> emitterType, T emitter);

    /**
     * Registers an emitter of the given type together with the name of the connector its channel goes to.
     * <p>
     * The default implementation ignores the connector name and delegates to
     * {@link #register(String, Class, Object)}.
     */
    default <T> void register(String name, String connector, Class<T> emitterType, T emitter) {
        register(name, emitterType, emitter);
    }

    List<Publisher<? extends Message<?>>> getPublishers(String name);

    Emitter<?> getEmitter(String name);

    MutinyEmitter<?> getMutinyEmitter(String name);

    <T> T getEmitter(String name, Class<? super T> emitterType);

    List<Subscriber<? extends Message<?>>> getSubscribers(String name);

    Set<String> getIncomingNames();

    Set<String> getOutgoingNames();

    Set<String> getEmitterNames();

    /**
     * Returns all the emitters registered for the given channel.
     * <p>
     * The default implementation returns the {@link Emitter} and {@link MutinyEmitter} registered for the channel,
     * when they are {@link MessagePublisherProvider}s.
     */
    default List<MessagePublisherProvider<?>> getEmitters(String name) {
        List<MessagePublisherProvider<?>> emitters = new ArrayList<>();
        Emitter<?> emitter = getEmitter(name);
        if (emitter instanceof MessagePublisherProvider) {
            emitters.add((MessagePublisherProvider<?>) emitter);
        }
        MutinyEmitter<?> mutinyEmitter = getMutinyEmitter(name);
        if (mutinyEmitter instanceof MessagePublisherProvider && mutinyEmitter != emitter) {
            emitters.add((MessagePublisherProvider<?>) mutinyEmitter);
        }
        return emitters;
    }

    Map<String, Boolean> getIncomingChannels();

    Map<String, Boolean> getOutgoingChannels();

    void register(String name, PausableChannel pausable);

    PausableChannel getPausable(String name);

    default Map<String, PausableChannel> getPausableChannels() {
        return Map.of();
    }

    default String getIncomingConnectorName(String channel) {
        return null;
    }

    default String getOutgoingConnectorName(String channel) {
        return null;
    }

    default Map<String, String> getConnectorNames() {
        return Map.of();
    }

}
