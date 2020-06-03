package io.smallrye.reactive.messaging.mqtt;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mutiny.core.Vertx;

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
        boolean broadcast = config.getBroadcast();
        MqttFailureHandler.Strategy strategy = MqttFailureHandler.Strategy.from(config.getFailureStrategy());
        MqttFailureHandler onNack = createFailureHandler(strategy, config.getChannel());

        Clients.ClientHolder holder = Clients.getHolder(vertx, host, port, server, options);
        this.source = ReactiveStreams.fromPublisher(
                holder.connect()
                        .onItem().produceMulti(client -> client.subscribe(topic, qos)
                                .onItem().produceMulti(x -> {
                                    subscribed.set(true);
                                    return holder.stream()
                                            .transform().byFilteringItemsWith(m -> m.topicName().equals(topic))
                                            .onItem().apply(m -> new ReceivingMqttMessage(m, onNack));
                                }))
                        .then(multi -> {
                            if (broadcast) {
                                return multi.broadcast().toAllSubscribers();
                            }
                            return multi;
                        })
                        .on().cancellation(() -> subscribed.set(false))
                        .onFailure().invoke(t -> LOGGER.error("Unable to establish a connection with the MQTT broker", t)));
    }

    private MqttFailureHandler createFailureHandler(MqttFailureHandler.Strategy strategy, String channel) {
        switch (strategy) {
            case IGNORE:
                return new MqttIgnoreFailure(LOGGER, channel);
            case FAIL:
                return new MqttFailStop(LOGGER, channel);
            default:
                throw new IllegalArgumentException("Unknown failure strategy: " + strategy);
        }
    }

    PublisherBuilder<MqttMessage<?>> getSource() {
        return source;
    }

    boolean isSubscribed() {
        return subscribed.get();
    }
}
