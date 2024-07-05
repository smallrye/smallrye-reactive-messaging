package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;
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

    private static String sha512(String password) {
        if (password == null || password.isEmpty()) {
            return null;
        }
        MessageDigest messageDigest = getMessageDigest("SHA-512");
        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[16];
        random.nextBytes(salt);
        messageDigest.update(salt);
        StringBuilder sb = new StringBuilder();
        toHex(messageDigest.digest(password.getBytes(StandardCharsets.UTF_8)), sb);
        return sb.toString();
    }

    private static void toHex(byte[] digest, StringBuilder sb) {
        for (byte b : digest) {
            sb.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
        }
    }

    private static MessageDigest getMessageDigest(String alg) {
        try {
            return MessageDigest.getInstance(alg);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static ClientHolder getHolder(Vertx vertx, MqttClientSessionOptions options) {

        String host = options.getHostname();
        int port = options.getPort();
        String clientId = options.getClientId();
        String server = options.getServerName().orElse(null);
        String username = options.getUsername();
        String pwdDigest = sha512(options.getPassword());

        String id = username + ":" + pwdDigest + "@"
                + host + ":"
                + port
                + "<" + (server == null ? "" : server)
                + ">-[" + (clientId != null ? clientId : "") + "]";
        return clients.computeIfAbsent(id, key -> {
            log.infof("Create MQTT Client for %s.", id);
            MqttClientSession client = MqttClientSession.create(vertx.getDelegate(), options);
            return new ClientHolder(client);
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
        private final BroadcastProcessor<MqttPublishMessage> messages;

        public ClientHolder(MqttClientSession client) {
            this.client = client;
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
