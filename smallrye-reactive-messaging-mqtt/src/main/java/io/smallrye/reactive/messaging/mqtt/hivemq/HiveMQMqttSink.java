package io.smallrye.reactive.messaging.mqtt.hivemq;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.Mqtt3RxClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3PublishResult;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.converters.uni.UniRxConverters;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import io.smallrye.reactive.messaging.mqtt.SendingMqttMessage;
import io.smallrye.reactive.messaging.mqtt.Sink;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;

public class HiveMQMqttSink implements Sink {

    private final String topic;
    private final int qos;

    private final SubscriberBuilder<? extends Message<?>, Void> sink;
    private final AtomicBoolean connected = new AtomicBoolean();

    public HiveMQMqttSink(Vertx vertx, HiveMQMqttConnectorOutgoingConfiguration config) {
        topic = config.getTopic().orElseGet(config::getChannel);
        qos = config.getQos();

        AtomicReference<Mqtt3RxClient> reference = new AtomicReference<>();
        sink = ReactiveStreams.<Message<?>> builder()
                .flatMapCompletionStage(msg -> {
                    Mqtt3RxClient client = reference.get();
                    if (client != null) {
                        if (client.getState().isConnected()) {
                            connected.set(true);
                            return CompletableFuture.completedFuture(msg);
                        } else {
                            CompletableFuture<Message<?>> future = new CompletableFuture<>();
                            vertx.setPeriodic(100, id -> {
                                if (client.getState().isConnected()) {
                                    vertx.cancelTimer(id);
                                    connected.set(true);
                                    future.complete(msg);
                                }
                            });
                            return future;
                        }
                    } else {
                        return HiveMQClients.getConnectedClient(config)
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
                    Mqtt3Client c = reference.getAndSet(null);
                    if (c != null) {
                        connected.set(false);
                        c.toBlocking().disconnect();
                    }
                })
                .onError(log::errorWhileSendingMessageToBroker)
                .ignore();
    }

    private CompletionStage<?> send(AtomicReference<Mqtt3RxClient> reference, Message<?> msg) {
        Mqtt3RxClient client = reference.get();
        String actualTopicToBeUsed = this.topic;
        MqttQos actualQoS = MqttQos.fromCode(this.qos);
        boolean isRetain = false;

        if (msg instanceof SendingMqttMessage) {
            MqttMessage<?> mm = ((SendingMqttMessage<?>) msg);
            actualTopicToBeUsed = mm.getTopic() == null ? topic : mm.getTopic();
            actualQoS = MqttQos.fromCode(mm.getQosLevel() == null ? actualQoS.getCode() : mm.getQosLevel().value());
            isRetain = mm.isRetain();
        }

        if (actualTopicToBeUsed == null) {
            log.ignoringNoTopicSet();
            return CompletableFuture.completedFuture(msg);
        }

        final Flowable<Mqtt3PublishResult> publish = client.publish(Flowable.just(Mqtt3Publish.builder()
                .topic(actualTopicToBeUsed)
                .qos(actualQoS)
                .payload(convert(msg.getPayload()))
                .retain(isRetain)
                .build()));

        return Uni.createFrom().converter(UniRxConverters.fromFlowable(), publish)
                .onItemOrFailure().transformToUni((s, f) -> {
                    if (f != null) {
                        return Uni.createFrom().completionStage(msg.nack(f).thenApply(x -> msg));
                    } else {
                        return Uni.createFrom().completionStage(msg.ack().thenApply(x -> msg));
                    }
                })
                .subscribeAsCompletionStage();
    }

    private ByteBuffer convert(Object payload) {
        final Buffer buffer = toBuffer(payload);

        return ByteBuffer.wrap(buffer.getBytes());
    }

    private Buffer toBuffer(Object payload) {
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
