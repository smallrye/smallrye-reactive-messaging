package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.mqtt.internal.MqttHelpers;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSession;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;

public class MqttSink {

    private final String topic;
    private final int qos;

    private final SubscriberBuilder<? extends Message<?>, Void> sink;
    private final AtomicBoolean ready = new AtomicBoolean();

    public MqttSink(Vertx vertx, MqttConnectorOutgoingConfiguration config,
            Instance<MqttClientSessionOptions> instances) {
        MqttClientSessionOptions options = MqttHelpers.createClientOptions(config, instances);
        topic = config.getTopic().orElseGet(config::getChannel);
        qos = config.getQos();

        AtomicReference<Clients.ClientHolder> reference = new AtomicReference<>();
        sink = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(msg -> {
                    Clients.ClientHolder client = reference.get();
                    if (client == null) {
                        client = Clients.getHolder(vertx, options);
                        reference.set(client);
                    }

                    return client.start()
                            .onSuccess(ignore -> ready.set(true))
                            .map(ignore -> msg)
                            .toCompletionStage();
                })
                .flatMapCompletionStage(msg -> send(reference, msg))
                .onComplete(() -> {
                    Clients.ClientHolder c = reference.getAndSet(null);
                    if (c != null) {
                        c.close()
                                .onComplete(ignore -> ready.set(false));
                    }
                })
                .onError(log::errorWhileSendingMessageToBroker)
                .ignore();
    }

    private CompletionStage<?> send(AtomicReference<Clients.ClientHolder> reference, Message<?> msg) {
        MqttClientSession client = reference.get().getClient();
        final String actualTopicToBeUsed;
        final MqttQoS actualQoS;
        final boolean isRetain;

        Optional<SendingMqttMessageMetadata> metadata = msg.getMetadata(SendingMqttMessageMetadata.class);
        if (metadata.isPresent()) {
            SendingMqttMessageMetadata mm = metadata.get();
            actualTopicToBeUsed = mm.getTopic() == null ? this.topic : mm.getTopic();
            actualQoS = mm.getQosLevel() == null ? MqttQoS.valueOf(this.qos) : mm.getQosLevel();
            isRetain = mm.isRetain();
        } else {
            actualTopicToBeUsed = this.topic;
            isRetain = false;
            actualQoS = MqttQoS.valueOf(this.qos);
        }

        if (actualTopicToBeUsed == null) {
            log.ignoringNoTopicSet();
            return CompletableFuture.completedFuture(msg);
        }

        return AsyncResultUni
                .<Integer> toUni(h -> client
                        .publish(actualTopicToBeUsed, convert(msg.getPayload()).getDelegate(), actualQoS, false, isRetain)
                        .onComplete(h))
                .onItemOrFailure().transformToUni((s, f) -> {
                    if (f != null) {
                        return Uni.createFrom().completionStage(msg.nack(f).thenApply(x -> msg));
                    } else {
                        OutgoingMessageMetadata.setResultOnMessage(msg, s);
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
        return ready.get();
    }
}
