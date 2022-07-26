package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder;
import io.smallrye.reactive.messaging.mqtt.internal.MqttHelpers;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSession;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;

public class MqttSink {

    private final String channel;
    private final String topic;
    private final int qos;
    private final boolean healthEnabled;

    private final Flow.Subscriber<? extends Message<?>> sink;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean alive = new AtomicBoolean();
    private final AtomicReference<Clients.ClientHolder> reference = new AtomicReference<>();

    public MqttSink(Vertx vertx, MqttConnectorOutgoingConfiguration config,
            Instance<MqttClientSessionOptions> instances) {

        MqttClientSessionOptions options = MqttHelpers.createClientOptions(config, instances);

        channel = config.getChannel();
        topic = config.getTopic().orElse(channel);
        qos = config.getQos();
        healthEnabled = config.getHealthEnabled();

        sink = MultiUtils.via(m -> m.onSubscription()
                .call(() -> {
                    Clients.ClientHolder client = reference.get();
                    if (client == null) {
                        client = Clients.getHolder(vertx, options);
                        reference.set(client);
                    }
                    return AsyncResultUni.<Void> toUni(h -> reference.get().start().onComplete(h))
                            .onItem().invoke(() -> {
                                started.set(true);
                                alive.set(true);
                            });
                })
                .onItem().transformToUniAndConcatenate(this::send)
                .onCompletion().invoke(() -> {
                    Clients.ClientHolder c = reference.getAndSet(null);
                    if (c != null)
                        c.close();
                    alive.set(false);
                })
                .onFailure().invoke(e -> {
                    alive.set(false);
                    log.errorWhileSendingMessageToBroker(e);
                }));
    }

    private Uni<? extends Message<?>> send(Message<?> msg) {
        final MqttClientSession client = reference.get().getClient();
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
            return Uni.createFrom().item(msg);
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
                });
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

    public Flow.Subscriber<? extends Message<?>> getSink() {
        return sink;
    }

    private boolean isConnected() {
        return reference.get() != null && reference.get().getClient().isConnected();
    }

    public void isStarted(HealthReportBuilder builder) {
        if (healthEnabled)
            builder.add(channel, started.get());
    }

    public void isReady(HealthReportBuilder builder) {
        if (healthEnabled)
            builder.add(channel, isConnected());
    }

    public void isAlive(HealthReportBuilder builder) {
        if (healthEnabled)
            builder.add(channel, alive.get());
    }

}
