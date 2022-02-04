package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;
import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.mqtt.session.RequestedQoS;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.mqtt.messages.MqttPublishMessage;

public class MqttSource {

    private final PublisherBuilder<MqttMessage<?>> source;
    private final AtomicBoolean ready = new AtomicBoolean();
    private final Pattern pattern;

    public MqttSource(Vertx vertx, MqttConnectorIncomingConfiguration config) {
        MqttClientSessionOptions options = MqttHelpers.createMqttClientOptions(config);

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

        Clients.ClientHolder holder = Clients.getHolder(vertx, options);
        holder.start();
        holder.getClient()
                .subscribe(topic, RequestedQoS.valueOf(qos))
                .onComplete(outcome -> log.info("Subscription outcome: " + outcome))
                .onSuccess(ignore -> ready.set(true));
        this.source = ReactiveStreams.fromPublisher(
                holder.stream()
                        .select().where(m -> matches(topic, m))
                        .onItem().transform(m -> new ReceivingMqttMessage(m, onNack))
                        .stage(multi -> {
                            if (broadcast) {
                                return multi.broadcast().toAllSubscribers();
                            }
                            return multi;
                        })
                        .onOverflow().buffer()
                        .onCancellation().call(() -> {
                            ready.set(false);
                            return Uni
                                    .createFrom()
                                    .completionStage(holder.getClient()
                                            .unsubscribe(topic).toCompletionStage());
                        })
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

    public boolean isReady() {
        return ready.get();
    }

}
