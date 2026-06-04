package io.smallrye.reactive.messaging.rabbitmq.og.internals;

import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQLogging.log;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.health.HealthReport.ChannelInfo;
import io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.providers.helpers.SenderProcessor;
import io.smallrye.reactive.messaging.rabbitmq.og.ConnectionHolder;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQMessageConverter;
import io.smallrye.reactive.messaging.rabbitmq.og.tracing.RabbitMQOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.rabbitmq.og.tracing.RabbitMQTrace;
import io.vertx.core.Context;

/**
 * Outgoing RabbitMQ channel that publishes messages to an exchange.
 * Uses {@link SenderProcessor} to manage backpressure and graceful completion.
 */
public class OutgoingRabbitMQChannel implements ConfirmListener, ShutdownListener {

    private final RabbitMQConnectorOutgoingConfiguration configuration;
    private final ConnectionHolder connectionHolder;
    private final Instance<Map<String, ?>> configMaps;
    private final RabbitMQOpenTelemetryInstrumenter instrumenter;

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);

    private final Context outgoingContext;
    private volatile Channel channel;

    private final boolean publisherConfirms;
    private final String defaultRoutingKey;
    private final Long defaultTtl;
    private final int retryAttempts;
    private final int retryInterval;

    private final Map<Long, UniEmitter<? super Void>> pendingConfirms = new HashMap<>();

    private final SenderProcessor processor;
    private final Flow.Subscriber<? extends Message<?>> subscriber;

    public OutgoingRabbitMQChannel(
            ConnectionHolder connectionHolder,
            RabbitMQConnectorOutgoingConfiguration configuration,
            Instance<Map<String, ?>> configMaps,
            Instance<OpenTelemetry> openTelemetryInstance) {

        this.connectionHolder = connectionHolder;
        this.configuration = configuration;
        this.configMaps = configMaps;
        this.outgoingContext = connectionHolder.getOrCreateSharedChannelContext(configuration.getChannel());

        // Initialize tracing if enabled
        if (configuration.getTracingEnabled()) {
            this.instrumenter = RabbitMQOpenTelemetryInstrumenter.createForSender(openTelemetryInstance);
        } else {
            this.instrumenter = null;
        }

        this.publisherConfirms = configuration.getPublishConfirms();
        this.defaultRoutingKey = configuration.getDefaultRoutingKey();
        this.defaultTtl = configuration.getDefaultTtl().orElse(null);
        this.retryAttempts = configuration.getRetryOnFailAttempts();
        this.retryInterval = configuration.getRetryOnFailInterval();

        long requests = configuration.getMaxInflightMessages();
        if (requests <= 0) {
            requests = Long.MAX_VALUE;
        }
        this.processor = new SenderProcessor(requests, true,
                publisherConfirms ? this::writeMessageWithConfirm : this::writeMessage);
        this.subscriber = MultiUtils.via(processor, m -> m
                .onSubscription().call(this::initialize)
                .onFailure().invoke(t -> log.unableToCreatePublisher(configuration.getChannel(), t))
                .onCompletion().invoke(() -> {
                    log.publisherComplete(configuration.getChannel());
                    completed.set(true);
                }));

        if (!configuration.getLazyClient()) {
            connectionHolder.connect()
                    .await().atMost(Duration.ofMillis(configuration.getConnectionTimeout()));
        }
    }

    public Flow.Subscriber<? extends Message<?>> getSink() {
        return subscriber;
    }

    private Uni<Void> initialize() {
        if (initialized.compareAndSet(false, true)) {
            return connectionHolder.connect()
                    .chain(conn -> Uni.createFrom().<Void> item(() -> {
                        try {
                            channel = connectionHolder.getOrCreateSharedChannel(configuration.getChannel());

                            // Set up topology
                            setupTopology();

                            // Enable publisher confirms if configured
                            if (publisherConfirms) {
                                channel.confirmSelect();
                                channel.addConfirmListener(this);
                                channel.addShutdownListener(this);
                                log.publisherConfirmsEnabled(configuration.getChannel());
                            }

                            log.publisherReady(configuration.getChannel());
                            return null;
                        } catch (Exception e) {
                            channel = null;
                            initialized.set(false);
                            throw new RuntimeException("Failed to initialize outgoing channel", e);
                        }
                    }).runSubscriptionOn(command -> outgoingContext.runOnContext(x -> command.run())));
        } else {
            return Uni.createFrom().voidItem();
        }
    }

    private void setupTopology() throws IOException {
        // Declare exchange if needed
        RabbitMQClientHelper.declareExchangeIfNeeded(channel, configuration, configMaps);
        log.topologyEstablished(configuration.getChannel(),
                RabbitMQClientHelper.getExchangeName(configuration));
    }

    @Override
    public void handleAck(long deliveryTag, boolean multiple) {
        outgoingContext.runOnContext(v -> handleConfirm(deliveryTag, multiple, true));

    }

    @Override
    public void handleNack(long deliveryTag, boolean multiple) {
        outgoingContext.runOnContext(v -> handleConfirm(deliveryTag, multiple, false));
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        if (!cause.isInitiatedByApplication()) {
            outgoingContext.runOnContext(v -> {
                for (UniEmitter<? super Void> emitter : pendingConfirms.values()) {
                    emitter.fail(cause);
                }
                pendingConfirms.clear();
            });
        }
    }

    private Uni<Void> writeMessageWithConfirm(Message<?> message) {
        // Capture seqNo, register emitter, and publish atomically on the event loop.
        Uni<Void> confirmed = Uni.createFrom().emitter(emitter -> {
            outgoingContext.runOnContext(v -> {
                long seqNo = channel.getNextPublishSeqNo();
                try {
                    pendingConfirms.put(seqNo, emitter);
                    message.getMetadata(OutgoingMessageMetadata.class).ifPresent(m -> m.setResult(seqNo));
                    publishMessage(message);
                } catch (Exception e) {
                    pendingConfirms.remove(seqNo);
                    emitter.fail(e);
                }
            });
        });

        if (retryAttempts > 0) {
            confirmed = confirmed.onFailure().retry()
                    .withBackOff(Duration.ofSeconds(retryInterval))
                    .atMost(retryAttempts);
        }

        return confirmed.chain(acked -> Uni.createFrom().completionStage(message.ack()))
                .onFailure().recoverWithUni(t -> Uni.createFrom()
                        .completionStage(message.nack(new RuntimeException("Message nacked by broker", t))));
    }

    private Uni<Void> writeMessage(Message<?> message) {
        Uni<Void> write = Uni.createFrom().<Void> item(() -> {
            try {
                publishMessage(message);
                return null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to publish message", e);
            }
        }).runSubscriptionOn(command -> outgoingContext.runOnContext(x -> command.run()));

        if (retryAttempts > 0) {
            write = write.onFailure().retry()
                    .withBackOff(Duration.ofSeconds(retryInterval))
                    .atMost(retryAttempts);
        }

        return write.chain(() -> Uni.createFrom().completionStage(message.ack()));
    }

    private void publishMessage(Message<?> message) throws IOException {
        // Use converter to transform message
        RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = RabbitMQMessageConverter
                .convert(message, defaultRoutingKey, Optional.ofNullable(defaultTtl));

        // Get exchange - use from metadata or default
        String exchange = converted.getExchange()
                .orElse(RabbitMQClientHelper.getExchangeName(configuration));

        String routingKey = converted.getRoutingKey();
        byte[] body = converted.getBody();
        AMQP.BasicProperties properties = converted.getProperties();

        // Apply tracing if enabled - inject trace context into headers
        if (configuration.getTracingEnabled() && instrumenter != null) {
            Map<String, Object> headers = new HashMap<>();
            if (properties.getHeaders() != null) {
                headers.putAll(properties.getHeaders());
            }
            RabbitMQTrace trace = RabbitMQTrace.traceExchange(exchange, routingKey, headers);
            instrumenter.traceOutgoing(message, trace);
            // Rebuild properties with tracing headers
            properties = properties.builder()
                    .headers(headers)
                    .build();
        }

        log.sendingMessageToExchange(exchange, routingKey);
        channel.basicPublish(exchange, routingKey, properties, body);
    }

    private void handleConfirm(long sequenceNumber, boolean multiple, boolean ack) {
        if (multiple) {
            pendingConfirms.entrySet().removeIf(entry -> {
                if (entry.getKey() <= sequenceNumber) {
                    resolveEmitter(entry.getValue(), ack);
                    return true;
                }
                return false;
            });
        } else {
            UniEmitter<? super Void> emitter = pendingConfirms.remove(sequenceNumber);
            if (emitter != null) {
                resolveEmitter(emitter, ack);
            }
        }
    }

    private void resolveEmitter(UniEmitter<? super Void> emitter, boolean ack) {
        if (ack) {
            emitter.complete(null);
        } else {
            emitter.fail(new RuntimeException("Message nacked by broker"));
        }
    }

    public boolean isHealthy() {
        // After the publisher stream completes/errors, report health based on
        // connection state only (the channel is intentionally closed after completion)
        if (completed.get()) {
            return connectionHolder.isConnected();
        }
        return connectionHolder.isConnected() && channel != null && channel.isOpen();
    }

    /**
     * Health check for liveness.
     */
    public HealthReportBuilder isAlive(HealthReportBuilder builder) {
        if (!configuration.getHealthEnabled()) {
            return builder;
        }

        return builder.add(new ChannelInfo(configuration.getChannel(), isHealthy()));
    }

    /**
     * Health check for readiness.
     */
    public HealthReportBuilder isReady(HealthReportBuilder builder) {
        if (!configuration.getHealthEnabled() || !configuration.getHealthReadinessEnabled()) {
            return builder;
        }

        return builder.add(new ChannelInfo(configuration.getChannel(), isHealthy()));
    }

    public void closeQuietly() {
        processor.cancel();
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (Exception e) {
            log.unableToCloseChannel(configuration.getChannel(), e);
        }
    }
}
