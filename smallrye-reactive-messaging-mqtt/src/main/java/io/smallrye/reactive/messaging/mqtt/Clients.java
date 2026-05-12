package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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

    static ClientHolder getHolder(Vertx vertx, MqttClientSessionOptions options) {

        String host = options.getHostname();
        int port = options.getPort();
        String clientId = Optional.ofNullable(options.getClientId()).orElse("");
        String server = options.getServerName().orElse("");
        String username = options.getUsername();
        boolean ssl = options.isSsl();
        boolean trustAll = options.isTrustAll();
        String willTopic = Optional.ofNullable(options.getWillTopic()).orElse("");
        int willQoS = options.getWillQoS();
        boolean willRetain = options.isWillRetain();
        int version = options.getVersion();

        String id = String.format("%s@%s:%s<%s>-[%s]-ssl:%s-trustAll:%s-will:%s:%d:%s-v:%d",
                username, host, port, server, clientId, ssl, trustAll, willTopic, willQoS, willRetain, version);

        return clients.computeIfAbsent(id, key -> {
            log.infof("Create MQTT Client for %s", id);
            MqttClientSession client = MqttClientSession.create(vertx.getDelegate(), options);
            return new ClientHolder(client, options);
        });
    }

    /**
     * Remove all the stored clients.
     */
    public static void clear() {
        clients.values().forEach(ClientHolder::close);
        clients.clear();
    }

    public static class ClientHolder {

        private final MqttClientSession client;
        private final MqttClientSessionOptions options;
        private final BroadcastProcessor<MqttPublishMessage> messages;

        public ClientHolder(MqttClientSession client, MqttClientSessionOptions options) {
            this.client = client;
            this.options = options;
            messages = BroadcastProcessor.create();
            client.messageHandler(m -> messages.onNext(MqttPublishMessage.newInstance(m)));
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
