package io.smallrye.reactive.messaging.rabbitmq.internals;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;
import static io.smallrye.reactive.messaging.rabbitmq.internals.RabbitMQClientHelper.parseArguments;
import static io.smallrye.reactive.messaging.rabbitmq.internals.RabbitMQClientHelper.serverQueueName;

import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.rabbitmq.client.AMQP;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.smallrye.reactive.messaging.rabbitmq.ClientHolder;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAck;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAutoAck;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.smallrye.reactive.messaging.rabbitmq.tracing.RabbitMQOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.rabbitmq.tracing.RabbitMQTrace;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;
import io.vertx.mutiny.rabbitmq.RabbitMQConsumer;
import io.vertx.rabbitmq.QueueOptions;

public class IncomingRabbitMQChannel {

    private final RabbitMQOpenTelemetryInstrumenter instrumenter;
    private final AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();
    private volatile RabbitMQClient client;
    private final RabbitMQConnectorIncomingConfiguration config;
    private final Multi<? extends Message<?>> stream;
    private final RabbitMQConnector connector;

    public IncomingRabbitMQChannel(RabbitMQConnector connector,
            RabbitMQConnectorIncomingConfiguration ic, Instance<OpenTelemetry> openTelemetryInstance) {
        if (ic.getTracingEnabled()) {
            instrumenter = RabbitMQOpenTelemetryInstrumenter
                    .createForConnector(openTelemetryInstance);
        } else {
            instrumenter = null;
        }
        this.config = ic;
        this.connector = connector;

        final RabbitMQFailureHandler onNack = createFailureHandler(connector.failureHandlerFactories(), ic);
        final RabbitMQAckHandler onAck = createAckHandler(ic);

        Multi<? extends Message<?>> multi = createConsumer(connector, ic)
                .invoke(tuple -> client = tuple.getItem1().client())
                // Translate all consumers into a merged stream of messages
                .onItem().transformToMulti(tuple -> getStreamOfMessages(tuple.getItem2(), tuple.getItem1(), ic, onNack, onAck));

        if (ic.getBroadcast()) {
            multi = multi.broadcast().toAllSubscribers();
        }

        this.stream = multi.onSubscription().invoke(subscription::set);
    }

    public Multi<? extends Message<?>> getStream() {
        return stream;
    }

    public HealthReport.HealthReportBuilder isAlive(HealthReport.HealthReportBuilder builder) {
        if (!config.getHealthEnabled()) {
            return builder;
        }

        return computeHealthReport(builder);
    }

    private HealthReport.HealthReportBuilder computeHealthReport(HealthReport.HealthReportBuilder builder) {
        if (config.getHealthLazySubscription()) {
            if (subscription.get() == null) {
                return builder.add(new HealthReport.ChannelInfo(config.getChannel(), true));
            }
        }

        if (client == null) {
            return builder.add(new HealthReport.ChannelInfo(config.getChannel(), false));
        }

        boolean alive = client.isConnected() && client.isOpenChannel();
        return builder.add(new HealthReport.ChannelInfo(config.getChannel(), alive));
    }

    public HealthReport.HealthReportBuilder isReady(HealthReport.HealthReportBuilder builder) {
        if (!config.getHealthEnabled() || !config.getHealthReadinessEnabled()) {
            return builder;
        }

        return computeHealthReport(builder);
    }

    private Uni<Tuple2<ClientHolder, RabbitMQConsumer>> createConsumer(RabbitMQConnector connector,
            RabbitMQConnectorIncomingConfiguration ic) {
        // Create a client
        final RabbitMQClient client = RabbitMQClientHelper.createClient(connector, ic);
        client.getDelegate().addConnectionEstablishedCallback(promise -> {

            Uni<Void> uni;
            if (ic.getMaxOutstandingMessages().isPresent()) {
                uni = client.basicQos(ic.getMaxOutstandingMessages().get(), false);
            } else {
                uni = Uni.createFrom().nullItem();
            }

            Instance<Map<String, ?>> maps = connector.configMaps();
            // Ensure we create the queues (and exchanges) from which messages will be read
            uni
                    .call(() -> declareQueue(client, ic, maps))
                    .call(() -> RabbitMQClientHelper.configureDLQorDLX(client, ic, maps))
                    .subscribe().with(ignored -> promise.complete(), promise::fail);
        });

        Context root = Context.newInstance(((VertxInternal) connector.vertx().getDelegate()).createEventLoopContext());
        final ClientHolder holder = new ClientHolder(client, ic, connector.vertx(), root);
        return holder.getOrEstablishConnection()
                .invoke(() -> log.connectionEstablished(ic.getChannel()))
                .flatMap(connection -> createConsumer(ic, connection).map(consumer -> Tuple2.of(holder, consumer)));
    }

    private RabbitMQFailureHandler createFailureHandler(Instance<RabbitMQFailureHandler.Factory> failureHandlerFactories,
            RabbitMQConnectorIncomingConfiguration config) {
        String strategy = config.getFailureStrategy();
        Instance<RabbitMQFailureHandler.Factory> failureHandlerFactory = CDIUtils.getInstanceById(failureHandlerFactories,
                strategy);
        if (failureHandlerFactory.isResolvable()) {
            return failureHandlerFactory.get().create(config, connector);
        } else {
            throw ex.illegalArgumentInvalidFailureStrategy(strategy);
        }
    }

    public RabbitMQAckHandler createAckHandler(RabbitMQConnectorIncomingConfiguration ic) {
        return (Boolean.TRUE.equals(ic.getAutoAcknowledgement())) ? new RabbitMQAutoAck(ic.getChannel())
                : new RabbitMQAck(ic.getChannel());
    }

    /**
     * Uses a {@link RabbitMQClient} to ensure the required queue-exchange bindings are created.
     *
     * @param client the RabbitMQ client
     * @param ic the incoming channel configuration
     * @return a {@link Uni<String>} which yields the queue name
     */
    private Uni<String> declareQueue(
            final RabbitMQClient client,
            final RabbitMQConnectorIncomingConfiguration ic,
            final Instance<Map<String, ?>> configMaps) {
        final String queueName = RabbitMQClientHelper.getQueueName(ic);

        // Declare the queue (and its binding(s) to the exchange, and TTL) if we have been asked to do so
        final JsonObject queueArgs = new JsonObject();
        Instance<Map<String, ?>> queueArguments = CDIUtils.getInstanceById(configMaps, ic.getQueueArguments());
        if (queueArguments.isResolvable()) {
            Map<String, ?> argsMap = queueArguments.get();
            argsMap.forEach(queueArgs::put);
        }
        if (ic.getAutoBindDlq()) {
            queueArgs.put("x-dead-letter-exchange", ic.getDeadLetterExchange());
            queueArgs.put("x-dead-letter-routing-key", ic.getDeadLetterRoutingKey().orElse(queueName));
        }
        ic.getQueueSingleActiveConsumer().ifPresent(sac -> queueArgs.put("x-single-active-consumer", sac));
        ic.getQueueXQueueType().ifPresent(queueType -> queueArgs.put("x-queue-type", queueType));
        ic.getQueueXQueueMode().ifPresent(queueMode -> queueArgs.put("x-queue-mode", queueMode));
        ic.getQueueTtl().ifPresent(queueTtl -> {
            if (queueTtl >= 0) {
                queueArgs.put("x-message-ttl", queueTtl);
            } else {
                throw ex.illegalArgumentInvalidQueueTtl();
            }
        });
        //x-max-priority
        ic.getQueueXMaxPriority().ifPresent(maxPriority -> queueArgs.put("x-max-priority", maxPriority));
        //x-delivery-limit
        ic.getQueueXDeliveryLimit().ifPresent(deliveryLimit -> queueArgs.put("x-delivery-limit", deliveryLimit));

        return RabbitMQClientHelper.declareExchangeIfNeeded(client, ic, configMaps)
                .flatMap(v -> {
                    if (ic.getQueueDeclare()) {
                        // Declare the queue.
                        String serverQueueName = serverQueueName(queueName);

                        Uni<AMQP.Queue.DeclareOk> declare;
                        if (serverQueueName.isEmpty()) {
                            declare = client.queueDeclare(serverQueueName, false, true, true);
                        } else {
                            declare = client.queueDeclare(serverQueueName, ic.getQueueDurable(),
                                    ic.getQueueExclusive(), ic.getQueueAutoDelete(), queueArgs);
                        }

                        return declare
                                .invoke(() -> log.queueEstablished(queueName))
                                .onFailure().invoke(ex -> log.unableToEstablishQueue(queueName, ex))
                                .flatMap(x -> RabbitMQClientHelper.establishBindings(client, ic))
                                .replaceWith(queueName);
                    } else {
                        // Not declaring the queue, so validate its existence...
                        // Ensures RabbitMQClient is notified of invalid queues during connection cycle.
                        return client.messageCount(queueName)
                                .onFailure().invoke(log::unableToConnectToBroker)
                                .replaceWith(queueName);
                    }
                });
    }

    private Uni<RabbitMQConsumer> createConsumer(RabbitMQConnectorIncomingConfiguration ic, RabbitMQClient client) {
        QueueOptions queueOptions = new QueueOptions();
        queueOptions.setConsumerArguments(parseArguments(ic.getConsumerArguments()));
        ic.getConsumerTag().ifPresent(queueOptions::setConsumerTag);
        ic.getConsumerExclusive().ifPresent(queueOptions::setConsumerExclusive);
        return client.basicConsumer(serverQueueName(RabbitMQClientHelper.getQueueName(ic)), queueOptions
                .setAutoAck(ic.getAutoAcknowledgement())
                .setMaxInternalQueueSize(ic.getMaxIncomingInternalQueueSize())
                .setKeepMostRecent(ic.getKeepMostRecent()));
    }

    private Multi<? extends Message<?>> getStreamOfMessages(
            RabbitMQConsumer receiver,
            ClientHolder holder,
            RabbitMQConnectorIncomingConfiguration ic,
            RabbitMQFailureHandler onNack,
            RabbitMQAckHandler onAck) {

        final String queueName = RabbitMQClientHelper.getQueueName(ic);
        final String contentTypeOverride = ic.getContentTypeOverride().orElse(null);
        log.receiverListeningAddress(queueName);

        Multi<IncomingRabbitMQMessage<?>> multi = receiver.toMulti()
                // close the consumer on stream termination
                .onTermination().call(receiver::cancel)
                .emitOn(c -> VertxContext.runOnContext(holder.getContext().getDelegate(), c))
                .map(m -> new IncomingRabbitMQMessage<>(m, holder, onNack, onAck, contentTypeOverride));
        if (ic.getTracingEnabled()) {
            return multi.map(msg -> instrumenter.traceIncoming(msg,
                    RabbitMQTrace.traceQueue(queueName, msg.message.envelope().getRoutingKey(), msg.getHeaders())));
        } else {
            return multi;
        }
    }

    public void terminate() {
        Flow.Subscription sub = subscription.getAndSet(null);
        if (sub != null) {
            sub.cancel();
        }
    }

}
