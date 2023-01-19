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
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder;
import io.smallrye.reactive.messaging.mqtt.Clients.ClientHolder;
import io.smallrye.reactive.messaging.mqtt.internal.MqttHelpers;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
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

    private final SubscriberBuilder<? extends Message<?>, Void> sink;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean ready = new AtomicBoolean();
    private final AtomicReference<Clients.ClientHolder> reference = new AtomicReference<>();

    public MqttSink(Vertx vertx, MqttConnectorOutgoingConfiguration config,
            Instance<MqttClientSessionOptions> instances) {

        MqttClientSessionOptions options = MqttHelpers.createClientOptions(config, instances);

        channel = config.getChannel();
        topic = config.getTopic().orElse(channel);
        qos = config.getQos();
        healthEnabled = config.getHealthEnabled();

        sink = ReactiveStreams.<Message<?>> builder()
                .via(new ConnectOnSubscribeProcessor(vertx, options))
                .flatMapCompletionStage(msg -> send(msg))
                .onError(log::errorWhileSendingMessageToBroker)
                .ignore();

    }

    /*
     * This processor let che client mqtt to connect on su
     */
    private class ConnectOnSubscribeProcessor implements Processor<Message<?>, Message<?>> {

        private Vertx vertx;
        private MqttClientSessionOptions options;
        private Subscriber<? super Message<?>> subscriber;
        private Subscription subscription;
        private long requestedItem;
        private Object lock = new Object();

        public ConnectOnSubscribeProcessor(Vertx vertx, MqttClientSessionOptions options) {
            this.vertx = vertx;
            this.options = options;
        }

        @Override
        public void subscribe(Subscriber<? super Message<?>> subscriber) {
            this.subscriber = subscriber;
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    if (ready.get()) {
                        subscription.request(n);
                    } else {
                        synchronized (lock) {
                            if (ready.get()) {
                                subscription.request(n);
                            } else {
                                requestedItem = n;
                            }
                        }
                    }
                }

                @Override
                public void cancel() {
                    subscription.cancel();
                }
            });
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            ClientHolder client = Clients.getHolder(vertx, options);
            reference.set(client);
            client.start().onSuccess(ignore -> {
                started.set(true);
                synchronized (lock) {
                    ready.set(true);
                    if (requestedItem > 0)
                        subscription.request(requestedItem);
                }
            }).toCompletionStage();
        }

        @Override
        public void onNext(Message<?> t) {
            subscriber.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            subscriber.onError(t);
        }

        @Override
        public void onComplete() {
            subscriber.onComplete();
            Clients.ClientHolder c = reference.getAndSet(null);
            if (c != null) {
                c.close()
                        .onComplete(ignore -> ready.set(false));
            }
        }
    };

    private CompletionStage<?> send(Message<?> msg) {

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
                .<Integer> toUni(h -> {
            reference.get().getClient()
                    .publish(actualTopicToBeUsed, convert(msg.getPayload()).getDelegate(), actualQoS, false, isRetain)
                    .onComplete(h);
                })
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

    public void isStarted(HealthReportBuilder builder) {
        if (healthEnabled)
            builder.add(channel, started.get());
    }

    public void isReady(HealthReportBuilder builder) {
        if (healthEnabled)
            builder.add(channel, ready.get());
    }

    public void isAlive(HealthReportBuilder builder) {
        if (healthEnabled)
            builder.add(channel, reference == null ? false : reference.get().getClient().isConnected());
    }

}
