package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.mqtt.MqttClient;

public class MqttSink implements Sink {

    private final String host;
    private final int port;
    private final String server;
    private final String topic;
    private final int qos;

    private final SubscriberBuilder<? extends Message<?>, Void> sink;
    private final AtomicBoolean connected = new AtomicBoolean();

    public MqttSink(Vertx vertx, MqttConnectorOutgoingConfiguration config) {
        MqttClientOptions options = MqttHelpers.createMqttClientOptions(config);
        host = config.getHost();
        int def = options.isSsl() ? 8883 : 1883;
        port = config.getPort().orElse(def);
        server = config.getServerName().orElse(null);
        topic = config.getTopic().orElseGet(config::getChannel);
        qos = config.getQos();

        AtomicReference<MqttClient> reference = new AtomicReference<>();
        sink = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(msg -> {
                    MqttClient client = reference.get();
                    if (client != null) {
                        if (client.isConnected()) {
                            connected.set(true);
                            return CompletableFuture.completedFuture(msg);
                        } else {
                            CompletableFuture<Message<?>> future = new CompletableFuture<>();
                            vertx.setPeriodic(100, id -> {
                                if (client.isConnected()) {
                                    vertx.cancelTimer(id);
                                    connected.set(true);
                                    future.complete(msg);
                                }
                            });
                            return future;
                        }
                    } else {
                        return Clients.getConnectedClient(vertx, host, port, server, options)
                                .map(c -> {
                                    reference.set(c);
                                    connected.set(true);
                                    return msg;
                                })
                                .subscribeAsCompletionStage();
                    }
                })
                .flatMapCompletionStage(msg -> send(reference, msg))
                .onComplete(() -> {
                    MqttClient c = reference.getAndSet(null);
                    if (c != null) {
                        connected.set(false);
                        c.disconnectAndForget();
                    }
                })
                .onError(log::errorWhileSendingMessageToBroker)
                .ignore();
    }

    private CompletionStage<?> send(AtomicReference<MqttClient> reference, Message<?> msg) {
        MqttClient client = reference.get();
        String actualTopicToBeUsed = this.topic;
        MqttQoS actualQoS = MqttQoS.valueOf(this.qos);
        boolean isRetain = false;

        if (msg instanceof SendingMqttMessage) {
            MqttMessage<?> mm = ((SendingMqttMessage<?>) msg);
            actualTopicToBeUsed = mm.getTopic() == null ? topic : mm.getTopic();
            actualQoS = mm.getQosLevel() == null ? actualQoS : mm.getQosLevel();
            isRetain = mm.isRetain();
        }

        if (actualTopicToBeUsed == null) {
            log.ignoringNoTopicSet();
            return CompletableFuture.completedFuture(msg);
        }

        return client.publish(actualTopicToBeUsed, convert(msg.getPayload()), actualQoS, false, isRetain)
                .onItemOrFailure().transformToUni((s, f) -> {
                    if (f != null) {
                        return Uni.createFrom().completionStage(msg.nack(f).thenApply(x -> msg));
                    } else {
                        return Uni.createFrom().completionStage(msg.ack().thenApply(x -> msg));
                    }
                })
                .subscribeAsCompletionStage();
    }

    private Buffer convert(Object payload) {
        if (payload instanceof JsonObject) {
            return new Buffer(((JsonObject) payload).toBuffer());
        }
        if (payload instanceof JsonArray) {
            return new Buffer(((JsonArray) payload).toBuffer());
        }
        if (payload instanceof String || payload.getClass().isPrimitive()) {
            return new Buffer(io.vertx.core.buffer.Buffer.buffer(payload.toString()));
        }
        if (payload instanceof byte[]) {
            return new Buffer(io.vertx.core.buffer.Buffer.buffer((byte[]) payload));
        }
        if (payload instanceof Buffer) {
            return (Buffer) payload;
        }
        if (payload instanceof io.vertx.core.buffer.Buffer) {
            return new Buffer((io.vertx.core.buffer.Buffer) payload);
        }
        // Convert to Json
        return new Buffer(Json.encodeToBuffer(payload));
    }

    public SubscriberBuilder<? extends Message<?>, Void> getSink() {
        return sink;
    }

    public boolean isReady() {
        return connected.get();
    }
}
