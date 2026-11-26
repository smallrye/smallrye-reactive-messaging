package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.inject.Instance;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSession;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.vertx.core.Future;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

public class Clients {

    private static final Map<String, ClientHolder> clients = new ConcurrentHashMap<>();

    private Clients() {
        // avoid direct instantiation.
    }

    private static String id(MqttClientSessionOptions options) {
        String host = options.getHostname();
        int port = options.getPort();
        String clientId = Optional.ofNullable(options.getClientId()).orElse("");
        String server = options.getServerName().orElse("");
        String username = options.getUsername();
        int version = options.getVersion();
        return String.format("%s@%s:%s<%s>-[%s]-v%d", username, host, port, server, clientId, version);
    }

    private static ClientHolder create(String id, Vertx vertx, MqttClientSessionOptions options,
            Instance<MqttClientSessionCustomizer> sessionCustomizers) {
        log.infof("Create MQTT Client for %s", id);
        MqttClientSession client = MqttClientSession.create(vertx.getDelegate(), options);
        if (sessionCustomizers != null) {
            for (MqttClientSessionCustomizer customizer : sessionCustomizers) {
                customizer.customize(client);
            }
        }
        return new ClientHolder(id, client);
    }

    /**
     * Gets the client for the options, creating it if needed, and retains it for the given channel. Atomic with
     * {@link #release(ClientHolder, String)} for the same client: a client is never handed out while its last channel
     * releases it.
     */
    static ClientHolder retain(Vertx vertx, MqttClientSessionOptions options,
            Instance<MqttClientSessionCustomizer> sessionCustomizers, String channel) {
        String id = id(options);
        return clients.compute(id,
                (key, current) -> (current != null ? current : create(id, vertx, options, sessionCustomizers))
                        .retain(channel));
    }

    /**
     * Releases the client for the given channel, and stops it if it was its last channel.
     *
     * @return a future completed when the client is stopped or still used by others
     */
    static Future<Void> release(ClientHolder holder, String channel) {
        if (holder.id == null) {
            // Not stored in the map.
            return holder.release(channel) ? holder.close() : Future.succeededFuture();
        }
        AtomicBoolean last = new AtomicBoolean();
        clients.computeIfPresent(holder.id, (key, current) -> {
            if (current == holder && current.release(channel)) {
                last.set(true);
                return null;
            }
            return current;
        });
        // Stopped outside the map update: a new channel gets a new client. A client no longer in the map was already
        // stopped by stopAll.
        return last.get() ? holder.close() : Future.succeededFuture();
    }

    /**
     * Remove all the stored clients.
     */
    public static void clear() {
        stopAll();
    }

    /**
     * Removes and stops all the stored clients.
     *
     * @return a future completed when every client is stopped, even if some failed to stop
     */
    static Future<Void> stopAll() {
        List<Future<Void>> stopping = new ArrayList<>();
        for (String id : List.copyOf(clients.keySet())) {
            ClientHolder holder = clients.remove(id);
            if (holder != null) {
                stopping.add(holder.close());
            }
        }
        return Future.join(stopping).mapEmpty();
    }

    public static class ClientHolder {

        // The key of the client in the map, null if it is not stored there.
        private final String id;
        private final MqttClientSession client;
        private final BroadcastProcessor<MqttPublishMessage> messages;
        private final Set<String> channels = ConcurrentHashMap.newKeySet();

        ClientHolder(String id, MqttClientSession client) {
            this.id = id;
            this.client = client;
            messages = BroadcastProcessor.create();
            client.messageHandler(m -> messages.onNext(MqttPublishMessage.newInstance(m)));
        }

        public ClientHolder retain(String channel) {
            channels.add(channel);
            return this;
        }

        public boolean release(String channel) {
            channels.remove(channel);
            return channels.isEmpty();
        }

        public Future<Void> start() {
            return client.start();
        }

        public Future<Void> close() {
            return client.stop();
        }

        public Multi<MqttPublishMessage> stream() {
            return messages;
        }

        public MqttClientSession getClient() {
            return client;
        }
    }

}
