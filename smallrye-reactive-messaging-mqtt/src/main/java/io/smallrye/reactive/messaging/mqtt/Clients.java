package io.smallrye.reactive.messaging.mqtt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.mqtt.MqttClient;
import io.vertx.mutiny.mqtt.messages.MqttConnAckMessage;
import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

public class Clients {

    private static Map<String, ClientHolder> clients = new ConcurrentHashMap<>();

    private Clients() {
        // avoid direct instantiation.
    }

    static Uni<MqttClient> getConnectedClient(Vertx vertx, String host, int port, String server,
            MqttClientOptions options) {

        String id = host + port + "<" + (server == null ? "" : server)
                + ">-[" + (options.getClientId() != null ? options.getClientId() : "") + "]";
        ClientHolder holder = clients.computeIfAbsent(id, key -> {
            MqttClient client = MqttClient.create(vertx, options);
            return new ClientHolder(client, host, port, server);
        });
        return holder.connect();
    }

    static ClientHolder getHolder(Vertx vertx, String host, int port, String server,
            MqttClientOptions options) {

        String id = host + port + "<" + (server == null ? "" : server)
                + ">-[" + (options.getClientId() != null ? options.getClientId() : "") + "]";
        return clients.computeIfAbsent(id, key -> {
            MqttClient client = MqttClient.create(vertx, options);
            return new ClientHolder(client, host, port, server);
        });
    }

    /**
     * Removed all the stored clients.
     */
    public static void clear() {
        clients.clear();
    }

    public static class ClientHolder {

        private final MqttClient client;
        private final Uni<MqttConnAckMessage> connection;
        private final BroadcastProcessor<MqttPublishMessage> messages;

        public ClientHolder(MqttClient client, String host, int port, String server) {
            this.client = client;
            this.connection = client.connect(port, host, server).cache();
            messages = BroadcastProcessor.create();
            client.publishHandler(messages::onNext);
            client.closeHandler(v -> messages.onComplete());
            client.exceptionHandler(messages::onError);
        }

        public Uni<MqttClient> connect() {
            return connection
                    .map(ignored -> client);
        }

        public Multi<MqttPublishMessage> stream() {
            return messages;
        }
    }

}
