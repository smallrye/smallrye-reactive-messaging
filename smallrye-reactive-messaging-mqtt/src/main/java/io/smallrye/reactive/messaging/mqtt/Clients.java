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
        private final ConcurrentHashMap<String, ChannelBuffer> buffers = new ConcurrentHashMap<>();

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
            ChannelBuffer buffer = new ChannelBuffer(bufferSize, pauseThresholdPercent, resumeThresholdPercent);
            buffers.put(channel, buffer);
            log.infof("[%s] Buffer size set to %d, pausing at %d (%d%%), resuming at %d (%d%%).",
                    channel, bufferSize, buffer.pauseThreshold, pauseThresholdPercent, buffer.resumeThreshold,
                    resumeThresholdPercent);
        }

        public void messageEnterBuffer(String channel) {
            ChannelBuffer buffer = buffers.get(channel);
            int count = buffer.messages.incrementAndGet();
            // Only pause/resume while the client is connected: acting on a disconnected
            // session has no effect, and the paused state could otherwise leak across reconnects.
            if (count > buffer.pauseThreshold && client.isConnected() && !client.isPaused()) {
                log.infof("[%s] Buffer almost full (%d messages), pausing MQTT message consumption.", channel, count);
                client.pause();
            }
        }

        public void messageExitBuffer(String channel) {
            buffers.get(channel).messages.decrementAndGet();
            if (client.isConnected() && client.isPaused() && allChannelsBelowResumeThreshold()) {
                log.info("All channels below resume threshold, resuming MQTT message consumption.");
                client.resume();
            }
        }

        private boolean allChannelsBelowResumeThreshold() {
            return buffers.values().stream()
                    .allMatch(ChannelBuffer::isBelowResumeThreshold);
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

        /**
         * The message buffer of a single channel. Channels connected to the same broker share the client, and so the
         * connection being paused, but each one has its own buffer and so its own thresholds.
         */
        private static class ChannelBuffer {

            private final AtomicInteger messages = new AtomicInteger();
            private final int pauseThreshold;
            private final int resumeThreshold;

            ChannelBuffer(int bufferSize, int pauseThresholdPercent, int resumeThresholdPercent) {
                this.pauseThreshold = bufferSize * pauseThresholdPercent / 100;
                this.resumeThreshold = bufferSize * resumeThresholdPercent / 100;
            }

            boolean isBelowResumeThreshold() {
                return messages.get() <= resumeThreshold;
            }
        }
    }

}
