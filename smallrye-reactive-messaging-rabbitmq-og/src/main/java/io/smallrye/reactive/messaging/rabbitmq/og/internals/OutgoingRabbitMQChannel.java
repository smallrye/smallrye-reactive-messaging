package io.smallrye.reactive.messaging.rabbitmq.og.internals;

import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQLogging.log;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.rabbitmq.og.ConnectionHolder;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.og.tracing.RabbitMQOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.rabbitmq.og.tracing.RabbitMQTrace;
import io.vertx.core.Context;

/**
 * Outgoing RabbitMQ channel that publishes messages to an exchange.
 * Handles topology setup, message publishing, publisher confirms, and backpressure.
 */
public class OutgoingRabbitMQChannel implements Subscriber<Message<?>> {

    private final RabbitMQConnectorOutgoingConfiguration configuration;
    private final ConnectionHolder connectionHolder;
    private final Instance<java.util.Map<String, ?>> configMaps;
    private final RabbitMQOpenTelemetryInstrumenter instrumenter;

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicLong inflightMessages = new AtomicLong(0);

    private Channel channel;
    private Context context;
    private Subscription subscription;

    private final boolean publisherConfirms;
    private final long maxInflightMessages;
    private final String defaultRoutingKey;
    private final Long defaultTtl;
    private final int retryAttempts;
    private final int retryInterval;

    // Track pending confirms: sequence number -> future
    private final ConcurrentHashMap<Long, CompletableFuture<Void>> pendingConfirms = new ConcurrentHashMap<>();

    public OutgoingRabbitMQChannel(
            ConnectionHolder connectionHolder,
            RabbitMQConnectorOutgoingConfiguration configuration,
            Instance<java.util.Map<String, ?>> configMaps,
            Instance<OpenTelemetry> openTelemetryInstance) {

        this.connectionHolder = connectionHolder;
        this.configuration = configuration;
        this.configMaps = configMaps;

        // Initialize tracing if enabled
        if (configuration.getTracingEnabled()) {
            this.instrumenter = RabbitMQOpenTelemetryInstrumenter.createForSender(openTelemetryInstance);
        } else {
            this.instrumenter = null;
        }

        this.publisherConfirms = configuration.getPublishConfirms();
        this.maxInflightMessages = configuration.getMaxInflightMessages();
        this.defaultRoutingKey = configuration.getDefaultRoutingKey();
        this.defaultTtl = configuration.getDefaultTtl().orElse(null);
        this.retryAttempts = configuration.getRetryOnFailAttempts();
        this.retryInterval = configuration.getRetryOnFailInterval();
    }

    private void initialize() {
        if (initialized.compareAndSet(false, true)) {
            connectionHolder.connect()
                    .subscribe().with(
                            conn -> {
                                try {
                                    // Create channel for publishing
                                    channel = connectionHolder.createChannel();
                                    context = connectionHolder.getContext();

                                    // Set up topology
                                    setupTopology();

                                    // Enable publisher confirms if configured
                                    if (publisherConfirms) {
                                        channel.confirmSelect();
                                        setupConfirmListeners();
                                        log.publisherConfirmsEnabled(configuration.getChannel());
                                    }

                                    log.publisherReady(configuration.getChannel());

                                    // Now that connection is ready, request messages
                                    if (subscription != null) {
                                        long initialRequest = Math.min(maxInflightMessages, 128);
                                        subscription.request(initialRequest);
                                    }
                                } catch (Exception e) {
                                    log.unableToCreatePublisher(configuration.getChannel(), e);
                                }
                            },
                            error -> log.unableToCreatePublisher(configuration.getChannel(), error));
        }
    }

    private void setupTopology() throws IOException {
        // Declare exchange if needed
        RabbitMQClientHelper.declareExchangeIfNeeded(channel, configuration, configMaps);
        log.topologyEstablished(configuration.getChannel(),
                RabbitMQClientHelper.getExchangeName(configuration));
    }

    private void setupConfirmListeners() throws IOException {
        ConfirmCallback ackCallback = (sequenceNumber, multiple) -> {
            context.runOnContext(v -> handleConfirm(sequenceNumber, multiple, true));
        };

        ConfirmCallback nackCallback = (sequenceNumber, multiple) -> {
            context.runOnContext(v -> handleConfirm(sequenceNumber, multiple, false));
        };

        channel.addConfirmListener(ackCallback, nackCallback);
    }

    private void handleConfirm(long sequenceNumber, boolean multiple, boolean ack) {
        if (multiple) {
            // Confirm all messages up to and including sequence number
            pendingConfirms.entrySet().removeIf(entry -> {
                if (entry.getKey() <= sequenceNumber) {
                    if (ack) {
                        entry.getValue().complete(null);
                    } else {
                        entry.getValue().completeExceptionally(new RuntimeException("Message nacked by broker"));
                    }
                    inflightMessages.decrementAndGet();
                    return true;
                }
                return false;
            });
        } else {
            // Confirm single message
            CompletableFuture<Void> future = pendingConfirms.remove(sequenceNumber);
            if (future != null) {
                if (ack) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(new RuntimeException("Message nacked by broker"));
                }
                inflightMessages.decrementAndGet();
            }
        }

        // Request more if we have capacity
        if (subscription != null && inflightMessages.get() < maxInflightMessages) {
            subscription.request(1);
        }
    }

    @Override
    public void onSubscribe(Subscription s) {
        this.subscription = s;
        initialize();
        // Messages are requested in the initialize success callback
        // after the connection is established
    }

    @Override
    public void onNext(Message<?> message) {
        // Check backpressure
        while (inflightMessages.get() >= maxInflightMessages) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                message.nack(e);
                return;
            }
        }

        inflightMessages.incrementAndGet();

        // Publish message with retry logic
        publishWithRetry(message, retryAttempts)
                .subscribe().with(
                        v -> {
                            // Success - ack handled by confirm callback or immediately
                        },
                        throwable -> {
                            log.messagePublishFailed(configuration.getChannel(), throwable);
                            message.nack(throwable).toCompletableFuture().join();
                            inflightMessages.decrementAndGet();
                            // Request more
                            if (subscription != null && inflightMessages.get() < maxInflightMessages) {
                                subscription.request(1);
                            }
                        });
    }

    private Uni<Void> publishWithRetry(Message<?> message, int remainingAttempts) {
        return Uni.createFrom().item(() -> {
            try {
                publishMessage(message);
                return (Void) null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to publish message", e);
            }
        })
                .runSubscriptionOn(command -> context.runOnContext(x -> command.run()))
                .onFailure().retry()
                .withBackOff(Duration.ofSeconds(retryInterval))
                .atMost(remainingAttempts);
    }

    private void publishMessage(Message<?> message) throws IOException {
        // Use converter to transform message
        io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQMessageConverter.OutgoingRabbitMQMessage converted = io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQMessageConverter
                .convert(message, defaultRoutingKey, java.util.Optional.ofNullable(defaultTtl));

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

        // Get next sequence number if using confirms
        Long sequenceNumber = null;
        if (publisherConfirms) {
            sequenceNumber = channel.getNextPublishSeqNo();
        }

        // Publish
        log.sendingMessageToExchange(exchange, routingKey);
        channel.basicPublish(exchange, routingKey, properties, body);

        // Handle ack based on confirms
        if (publisherConfirms && sequenceNumber != null) {
            // Create future for this publish
            final long seqNo = sequenceNumber;
            CompletableFuture<Void> confirmFuture = new CompletableFuture<>();
            pendingConfirms.put(seqNo, confirmFuture);

            // Set delivery tag in OutgoingMessageMetadata for interceptors
            message.getMetadata(OutgoingMessageMetadata.class)
                    .ifPresent(m -> m.setResult(seqNo));

            // Ack message when confirmed (non-blocking)
            Uni.createFrom().completionStage(confirmFuture)
                    .subscribe().with(
                            v -> message.ack(),
                            throwable -> message.nack(throwable));
        } else {
            // Immediate ack if not using confirms (non-blocking)
            Uni.createFrom().completionStage(message.ack())
                    .subscribe().with(
                            v -> {
                                inflightMessages.decrementAndGet();
                                // Request more
                                if (subscription != null && inflightMessages.get() < maxInflightMessages) {
                                    subscription.request(1);
                                }
                            });
        }
    }

    @Override
    public void onError(Throwable t) {
        log.publisherError(configuration.getChannel(), t);
        completed.set(true);
        cleanup();
    }

    @Override
    public void onComplete() {
        log.publisherComplete(configuration.getChannel());
        completed.set(true);
        cleanup();
    }

    public boolean isHealthy() {
        // After the publisher stream completes/errors, report health based on
        // connection state only (the channel is intentionally closed after completion)
        if (completed.get()) {
            return connectionHolder.isConnected();
        }
        return connectionHolder.isConnected() && channel != null && channel.isOpen();
    }

    public long getInflightMessages() {
        return inflightMessages.get();
    }

    /**
     * Health check for liveness.
     */
    public io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder isAlive(
            io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder builder) {
        if (!configuration.getHealthEnabled()) {
            return builder;
        }

        return computeHealthReport(builder);
    }

    /**
     * Health check for readiness.
     */
    public io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder isReady(
            io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder builder) {
        if (!configuration.getHealthEnabled() || !configuration.getHealthReadinessEnabled()) {
            return builder;
        }

        return computeHealthReport(builder);
    }

    private io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder computeHealthReport(
            io.smallrye.reactive.messaging.health.HealthReport.HealthReportBuilder builder) {
        boolean ok = isHealthy();
        return builder.add(new io.smallrye.reactive.messaging.health.HealthReport.ChannelInfo(
                configuration.getChannel(), ok));
    }

    private void cleanup() {
        try {
            if (channel != null && channel.isOpen()) {
                // Wait for pending confirms
                if (publisherConfirms && !pendingConfirms.isEmpty()) {
                    try {
                        channel.waitForConfirmsOrDie(5000);
                    } catch (Exception e) {
                        log.waitForConfirmsFailed(configuration.getChannel(), e);
                    }
                }

                channel.close();
            }
        } catch (Exception e) {
            log.unableToCloseChannel(configuration.getChannel(), e);
        }
    }
}
