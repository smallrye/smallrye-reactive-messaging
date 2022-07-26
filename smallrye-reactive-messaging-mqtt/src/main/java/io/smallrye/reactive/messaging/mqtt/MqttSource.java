package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;
import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import javax.enterprise.inject.Instance;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.mqtt.internal.MqttHelpers;
import io.smallrye.reactive.messaging.mqtt.internal.MqttTopicHelper;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.mqtt.session.RequestedQoS;
import io.vertx.mutiny.core.Vertx;

public class MqttSource {

    private final Flow.Publisher<ReceivingMqttMessage> source;
    private final AtomicBoolean ready = new AtomicBoolean();
    private final Pattern pattern;

    public MqttSource(Vertx vertx, MqttConnectorIncomingConfiguration config,
            Instance<MqttClientSessionOptions> instances) {
        MqttClientSessionOptions options = MqttHelpers.createClientOptions(config, instances);

        String topic = config.getTopic().orElseGet(config::getChannel);
        int qos = config.getQos();
        boolean broadcast = config.getBroadcast();
        MqttFailureHandler.Strategy strategy = MqttFailureHandler.Strategy.from(config.getFailureStrategy());
        MqttFailureHandler onNack = createFailureHandler(strategy, config.getChannel());

        if (topic.contains("#") || topic.contains("+")) {
            String replace = MqttTopicHelper.escapeTopicSpecialWord(MqttHelpers.rebuildMatchesWithSharedSubscription(topic))
                    .replace("+", "[^/]+")
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

        this.source = holder.stream()
                .select().where(m -> MqttTopicHelper.matches(topic, pattern, m))
                .onItem().transform(m -> new ReceivingMqttMessage(m, onNack))
                .stage(multi -> {
                    if (broadcast) {
                        return multi.broadcast().toAllSubscribers();
                    }
                    return multi;
                })
                .onOverflow().buffer(config.getBufferSize())
                .onCancellation().call(() -> {
                    ready.set(false);
                    if (config.getUnsubscribeOnDisconnection())
                        return Uni
                                .createFrom()
                                .completionStage(holder.getClient()
                                        .unsubscribe(topic).toCompletionStage());
                    else
                        return Uni.createFrom().voidItem();
                })
                .onFailure().invoke(log::unableToConnectToBroker);
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

    Flow.Publisher<ReceivingMqttMessage> getSource() {
        return source;
    }

    public boolean isReady() {
        return ready.get();
    }

}
