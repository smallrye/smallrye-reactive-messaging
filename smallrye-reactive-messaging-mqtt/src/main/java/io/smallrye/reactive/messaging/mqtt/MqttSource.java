package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;
import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import jakarta.enterprise.inject.Instance;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder;
import io.smallrye.reactive.messaging.mqtt.internal.MqttHelpers;
import io.smallrye.reactive.messaging.mqtt.internal.MqttTopicHelper;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.mqtt.session.RequestedQoS;
import io.vertx.mutiny.core.Vertx;

public class MqttSource {

    private final Flow.Publisher<ReceivingMqttMessage> source;
    private final AtomicBoolean ready = new AtomicBoolean();
    private final String channel;
    private final Pattern pattern;
    private final boolean healthEnabled;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean alive = new AtomicBoolean();
    private final Clients.ClientHolder holder;

    public MqttSource(Vertx vertx, MqttConnectorIncomingConfiguration config,
            Instance<MqttClientSessionOptions> instances) {
        MqttClientSessionOptions options = MqttHelpers.createClientOptions(config, instances);

        channel = config.getChannel();
        String topic = config.getTopic().orElse(channel);
        int qos = config.getQos();
        boolean broadcast = config.getBroadcast();
        healthEnabled = config.getHealthEnabled();

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

        holder = Clients.getHolder(vertx, options);
        holder.start().onSuccess(ignore -> started.set(true));
        holder.getClient()
                .subscribe(topic, RequestedQoS.valueOf(qos))
                .onFailure(outcome -> log.info("Subscription failed!"))
                .onSuccess(outcome -> {
                    log.info("Subscription success on topic " + topic + ", Max QoS " + outcome + ".");
                    alive.set(true);
                });

        this.source = holder.stream()
                .select().where(m -> MqttTopicHelper.matches(topic, pattern, m))
                .onItem().transform(m -> new ReceivingMqttMessage(m, onNack))
                .stage(multi -> {
                    if (broadcast)
                        return multi.broadcast().toAllSubscribers();

                    return multi;
                })
                .onOverflow().buffer(config.getBufferSize())
                .onCancellation().call(() -> {
                    alive.set(false);
                    if (config.getUnsubscribeOnDisconnection())
                        return Uni
                                .createFrom()
                                .completionStage(holder.getClient()
                                        .unsubscribe(topic).toCompletionStage());
                    else
                        return Uni.createFrom().voidItem();
                })
                .onFailure().invoke(e -> {
                    alive.set(false);
                    log.unableToConnectToBroker(e);
                });
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

    public void isStarted(HealthReportBuilder builder) {
        if (healthEnabled)
            builder.add(channel, started.get());
    }

    public void isReady(HealthReportBuilder builder) {
        if (healthEnabled)
            builder.add(channel, holder.getClient().isConnected());
    }

    public void isAlive(HealthReportBuilder builder) {
        if (healthEnabled)
            builder.add(channel, alive.get());
    }

}
