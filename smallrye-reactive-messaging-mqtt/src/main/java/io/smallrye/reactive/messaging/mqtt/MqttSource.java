package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;
import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.inject.Instance;

import io.netty.handler.codec.mqtt.MqttSubscriptionOption.RetainedHandlingPolicy;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder;
import io.smallrye.reactive.messaging.mqtt.internal.ActualTopicFilter;
import io.smallrye.reactive.messaging.mqtt.internal.MqttHelpers;
import io.smallrye.reactive.messaging.mqtt.internal.MqttTopicHelper;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.mqtt.session.RequestedQoS;
import io.smallrye.reactive.messaging.providers.helpers.ConfigUtils;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.core.Future;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class MqttSource {

    private final Flow.Publisher<ReceivingMqttMessage> source;
    private final AtomicBoolean ready = new AtomicBoolean();
    private final String channel;
    private final boolean healthEnabled;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean alive = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Clients.ClientHolder holder;
    private final String topic;
    private final boolean unsubscribeOnDisconnection;

    public MqttSource(Vertx vertx, MqttConnectorIncomingConfiguration config,
            Instance<ClientCustomizer<MqttClientSessionOptions>> configCustomizers,
            Instance<MqttClientSessionOptions> instances) {
        this(vertx, config, configCustomizers, instances, null);
    }

    public MqttSource(Vertx vertx, MqttConnectorIncomingConfiguration config,
            Instance<ClientCustomizer<MqttClientSessionOptions>> configCustomizers,
            Instance<MqttClientSessionOptions> instances,
            Instance<MqttClientSessionCustomizer> sessionCustomizers) {
        MqttClientSessionOptions options = ConfigUtils.customize(config.config(), configCustomizers,
                MqttHelpers.createClientOptions(config, instances));

        channel = config.getChannel();
        topic = config.getTopic().orElse(channel);
        unsubscribeOnDisconnection = config.getUnsubscribeOnDisconnection();
        int qos = config.getQos();
        boolean broadcast = config.getBroadcast();
        healthEnabled = config.getHealthEnabled();

        MqttFailureHandler.Strategy strategy = MqttFailureHandler.Strategy.from(config.getFailureStrategy());
        MqttFailureHandler onNack = createFailureHandler(strategy, config.getChannel());

        MqttTopicHelper.validateTopicFilter(topic);
        ActualTopicFilter actualTopicFilter = MqttTopicHelper.topicFilterToPattern(topic);

        boolean noLocal = config.getNoLocal();
        boolean retainAsPublished = config.getRetainAsPublished();
        RetainedHandlingPolicy retainHandling = MqttHelpers.retainedHandlingPolicy(config.getRetainHandling(), channel);
        Integer subscriptionIdentifier = config.getSubscriptionIdentifier().orElse(null);
        boolean hasV5SubOptions = noLocal || retainAsPublished
                || retainHandling != RetainedHandlingPolicy.SEND_AT_SUBSCRIBE || subscriptionIdentifier != null;

        final Context root = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        holder = Clients.retain(vertx, options, sessionCustomizers, channel);
        holder.start().onSuccess(ignore -> started.set(true));

        if (hasV5SubOptions) {
            holder.getClient()
                    .subscribe(topic, RequestedQoS.valueOf(qos), noLocal, retainAsPublished, retainHandling,
                            subscriptionIdentifier)
                    .onFailure(outcome -> log.info("Subscription failed!"))
                    .onSuccess(outcome -> {
                        log.info("Subscription success on topic " + topic + ", Max QoS " + outcome + ".");
                        alive.set(true);
                    });
        } else {
            holder.getClient()
                    .subscribe(topic, RequestedQoS.valueOf(qos))
                    .onFailure(outcome -> log.info("Subscription failed!"))
                    .onSuccess(outcome -> {
                        log.info("Subscription success on topic " + topic + ", Max QoS " + outcome + ".");
                        alive.set(true);
                    });
        }

        this.source = holder.stream()
                // Once closed, the channel gets nothing more, even if the shared client keeps receiving for others.
                .select().where(m -> !closed.get() && MqttTopicHelper.matches(actualTopicFilter, m.topicName()))
                .onOverflow().buffer(config.getBufferSize())
                .emitOn(c -> VertxContext.runOnContext(root.getDelegate(), c))
                .onItem().transform(m -> new ReceivingMqttMessage(m, onNack))
                .stage(multi -> {
                    if (broadcast)
                        return multi.broadcast().toAllSubscribers();

                    return multi;
                })
                .onCancellation().call(() -> {
                    alive.set(false);
                    if (unsubscribeOnDisconnection)
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

    /**
     * Closes this channel: it stops receiving messages, unsubscribes from its topic if
     * {@code unsubscribe-on-disconnection} is set, and releases the shared client, which is stopped once no channel
     * uses it anymore.
     *
     * @return a future completed when the client is stopped or still used by other channels
     */
    public Future<Void> close() {
        if (!closed.compareAndSet(false, true)) {
            return Future.succeededFuture();
        }
        alive.set(false);
        Future<Void> unsubscribed = unsubscribeOnDisconnection ? holder.getClient().unsubscribe(topic)
                : Future.succeededFuture();
        return unsubscribed.transform(ignored -> Clients.release(holder, channel));
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
