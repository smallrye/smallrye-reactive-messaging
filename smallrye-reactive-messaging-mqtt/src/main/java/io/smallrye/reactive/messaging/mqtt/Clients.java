package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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

    static ClientHolder getHolder(Vertx vertx, MqttClientSessionOptions options) {
        return getHolder(vertx, options, null);
    }

    static ClientHolder getHolder(Vertx vertx, MqttClientSessionOptions options,
            Instance<MqttClientSessionCustomizer> sessionCustomizers) {

        String host = options.getHostname();
        int port = options.getPort();
        String clientId = Optional.ofNullable(options.getClientId()).orElse("");
        String server = options.getServerName().orElse("");
        String username = options.getUsername();

        int version = options.getVersion();
        String id = String.format("%s@%s:%s<%s>-[%s]-v%d", username, host, port, server, clientId, version);
        return clients.computeIfAbsent(id, key -> {
            log.infof("Create MQTT Client for %s", id);
            MqttClientSession client = MqttClientSession.create(vertx.getDelegate(), options);
            if (sessionCustomizers != null) {
                for (MqttClientSessionCustomizer customizer : sessionCustomizers) {
                    customizer.customize(client);
                }
            }
            return new ClientHolder(client);
        });
    }

    static void release(ClientHolder holder) {
        clients.values().remove(holder);
        holder.close();
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
        private final Set<String> channels = ConcurrentHashMap.newKeySet();
        private final ConcurrentHashMap<String, AtomicInteger> messagesInBuffer = new ConcurrentHashMap<>();
        private int ninetiethThreshold;
        private int halfThreshold;

        public ClientHolder(MqttClientSession client) {
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

        public void registerChannelBuffer(String channel, int bufferSize, int pauseThresholdPercent,
                int resumeThresholdPercent) {
            messagesInBuffer.put(channel, new AtomicInteger(0));
            ninetiethThreshold = bufferSize * pauseThresholdPercent / 100;
            halfThreshold = bufferSize * resumeThresholdPercent / 100;
            log.infof("[%s] Buffer size set to %d, pausing at %d (%d%%), resuming at %d (%d%%).",
                    channel, bufferSize, ninetiethThreshold, pauseThresholdPercent, halfThreshold, resumeThresholdPercent);
        }

        public void messageEnterBuffer(String channel) {
            int count = messagesInBuffer.get(channel).incrementAndGet();
            // Only pause/resume while the client is connected: acting on a disconnected
            // session has no effect, and the paused state could otherwise leak across reconnects.
            if (count > ninetiethThreshold && client.isConnected() && !client.isPaused()) {
                log.infof("[%s] Buffer almost full, pausing MQTT message consumption.", channel);
                client.pause();
            }
        }

        public void messageExitBuffer(String channel) {
            messagesInBuffer.get(channel).decrementAndGet();
            if (client.isConnected() && client.isPaused() && allChannelsBelowHalf()) {
                log.info("All channels below resume threshold, resuming MQTT message consumption.");
                client.resume();
            }
        }

        private boolean allChannelsBelowHalf() {
            return messagesInBuffer.values().stream()
                    .allMatch(c -> c.get() <= halfThreshold);
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
