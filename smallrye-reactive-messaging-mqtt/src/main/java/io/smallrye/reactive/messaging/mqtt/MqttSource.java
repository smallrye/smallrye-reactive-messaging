package io.smallrye.reactive.messaging.mqtt;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.mqtt.MqttClient;

public class MqttSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttSource.class);
    private final PublisherBuilder<MqttMessage<?>> source;
    private final AtomicBoolean subscribed = new AtomicBoolean();

    public MqttSource(Vertx vertx, MqttConnectorIncomingConfiguration config) {
        MqttClientOptions options = MqttHelpers.createMqttClientOptions(config);

        String host = config.getHost();
        int def = options.isSsl() ? 8883 : 1883;
        int port = config.getPort().orElse(def);
        String server = config.getServerName().orElse(null);
        String topic = config.getTopic().orElseGet(config::getChannel);
        int qos = config.getQos();
        MqttClient client = MqttClient.create(vertx, options);
        boolean broadcast = config.getBroadcast();

        this.source = ReactiveStreams.fromPublisher(
                client.connect(port, host, server)
                        .onItem().produceMulti(a -> Multi.createFrom().<MqttMessage<?>> emitter(emitter -> {
                            client.publishHandler(message -> emitter.emit(new ReceivingMqttMessage(message)));

                            client.subscribe(topic, qos).subscribe().with(
                                    i -> subscribed.set(true),
                                    emitter::fail);

                        }, BackPressureStrategy.BUFFER))
                        .then(multi -> {
                            if (broadcast) {
                                return multi.broadcast().toAllSubscribers();
                            }
                            return multi;
                        })
                        .on().cancellation(() -> {
                            subscribed.set(false);
                            client.disconnectAndForget();
                        })
                        .onFailure().invoke(t -> LOGGER.error("Unable to establish a connection with the MQTT broker", t)));
    }

    PublisherBuilder<MqttMessage<?>> getSource() {
        return source;
    }

    boolean isSubscribed() {
        return subscribed.get();
    }
}
