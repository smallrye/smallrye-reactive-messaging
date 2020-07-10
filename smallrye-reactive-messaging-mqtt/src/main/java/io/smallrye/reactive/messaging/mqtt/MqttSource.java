package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;
import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

public class MqttSource {

    private final PublisherBuilder<MqttMessage<?>> source;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final Pattern pattern;

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

        if (topic.contains("#") || topic.contains("+")) {
            String replace = topic.replace("+", "[^/]+")
                    .replace("#", ".+");
            pattern = Pattern.compile(replace);
        } else {
            pattern = null;
        }

        Clients.ClientHolder holder = Clients.getHolder(vertx, host, port, server, options);
        this.source = ReactiveStreams.fromPublisher(
                holder.connect()
                        .onItem().transformToMulti(client -> client.subscribe(topic, qos)
                                .onItem().transformToMulti(x -> {
                                    subscribed.set(true);
                                    return holder.stream()
                                            .transform().byFilteringItemsWith(m -> matches(topic, m))
                                            .onItem().transform(m -> new ReceivingMqttMessage(m, onNack));
                                }))
                        .stage(multi -> {
                            if (broadcast) {
                                return multi.broadcast().toAllSubscribers();
                            }
                            return multi;
                        })
                        .on().cancellation(() -> subscribed.set(false))
                        .onFailure().invoke(log::unableToConnectToBroker));
    }

    private boolean matches(String topic, MqttPublishMessage m) {
        if (pattern != null) {
            return pattern.matcher(m.topicName()).matches();
        }
        return m.topicName().equals(topic);
    }

    private MqttFailureHandler createFailureHandler(MqttFailureHandler.Strategy strategy, String channel) {
        switch (strategy) {
            case IGNORE:
                return new MqttIgnoreFailure(channel);
            case FAIL:
                return new MqttFailStop(channel);
            default:
                throw ex.illegalArgumentUnknownStrategy(strategy.toString());
        }
    }

    PublisherBuilder<MqttMessage<?>> getSource() {
        return source;
    }

    boolean isSubscribed() {
        return subscribed.get();
    }
}
