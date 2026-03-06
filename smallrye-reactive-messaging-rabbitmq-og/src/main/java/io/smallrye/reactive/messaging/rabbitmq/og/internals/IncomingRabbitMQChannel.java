package io.smallrye.reactive.messaging.rabbitmq.og.internals;

import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQLogging.log;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.reactive.messaging.rabbitmq.og.ConnectionHolder;
import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQRejectMetadata;
import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAck;
import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAutoAck;
import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNack;
import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNackHandler;
import io.smallrye.reactive.messaging.rabbitmq.og.fault.RabbitMQFailureHandler;
import io.smallrye.reactive.messaging.rabbitmq.og.tracing.RabbitMQOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.rabbitmq.og.tracing.RabbitMQTrace;
import io.vertx.core.Context;

/**
 * Incoming RabbitMQ channel that consumes messages from a queue.
 * Handles topology setup, message consumption, acknowledgement, and backpressure.
 */
public class IncomingRabbitMQChannel {

    private final RabbitMQConnectorIncomingConfiguration configuration;
    private final ConnectionHolder connectionHolder;
    private final Instance<java.util.Map<String, ?>> configMaps;
    private final RabbitMQOpenTelemetryInstrumenter instrumenter;

    private final AtomicBoolean subscribed = new AtomicBoolean(false);
    private final AtomicInteger outstandingMessages = new AtomicInteger(0);
    private final AtomicReference<Channel> channelRef = new AtomicReference<>();
    private final AtomicReference<String> consumerTagRef = new AtomicReference<>();

    private final boolean autoAck;
    private final int maxOutstandingMessages;

    private Multi<? extends Message<?>> stream;

    public IncomingRabbitMQChannel(
            ConnectionHolder connectionHolder,
            RabbitMQConnectorIncomingConfiguration configuration,
            Instance<java.util.Map<String, ?>> configMaps,
            Instance<OpenTelemetry> openTelemetryInstance) {

        System.out.println("Creating IncomingRabbitMQChannel for channel: " + configuration.getChannel());
        this.connectionHolder = connectionHolder;
        this.configuration = configuration;
        this.configMaps = configMaps;

        // Initialize tracing if enabled
        if (configuration.getTracingEnabled()) {
            this.instrumenter = RabbitMQOpenTelemetryInstrumenter.createForConnector(openTelemetryInstance);
        } else {
            this.instrumenter = null;
        }

        this.autoAck = configuration.getAutoAcknowledgement();
        this.maxOutstandingMessages = configuration.getMaxOutstandingMessages().orElse(256);
    }

    /**
     * Get the message stream.
     */
    public Multi<? extends Message<?>> getStream() {
        if (stream == null) {
            stream = createStream();
        }
        return stream;
    }

    private Multi<? extends Message<?>> createStream() {
        // Determine if broadcast mode is needed
        boolean broadcast = configuration.getBroadcast();
        Context context = connectionHolder.getContext();

        Multi<Message<?>> messageStream;

        if (broadcast) {
            // Use BroadcastProcessor for broadcast mode
            BroadcastProcessor<Message<?>> processor = BroadcastProcessor.create();
            // Set up consumer immediately
            setupConsumer(processor::onNext, processor::onError, processor::onComplete);
            messageStream = processor;
        } else {
            // Use emitter with buffer for unicast mode to handle backpressure properly
            messageStream = Multi.createFrom().emitter(emitter -> {
                // Set up consumer immediately (not lazily)
                setupConsumer(
                        message -> {
                            // Emit message to subscriber
                            if (!emitter.isCancelled()) {
                                emitter.emit(message);
                            }
                        },
                        error -> {
                            if (!emitter.isCancelled()) {
                                emitter.fail(error);
                            }
                        },
                        () -> {
                            if (!emitter.isCancelled()) {
                                emitter.complete();
                            }
                        });
            }, BackPressureStrategy.BUFFER);
        }

        return messageStream
                // Ensure items are always dispatched on the Vert.x event loop thread
                // associated with this channel's context, regardless of where demand originates
                .emitOn(cmd -> context.runOnContext(v -> cmd.run()))
                .onItem().invoke(() -> log.messageReceived(configuration.getChannel()))
                .onFailure().invoke(t -> log.messageProcessingFailed(configuration.getChannel(), t))
                .onTermination().invoke(() -> cleanup());
    }

    private void setupConsumer(
            java.util.function.Consumer<Message<?>> onMessage,
            java.util.function.Consumer<Throwable> onError,
            Runnable onComplete) {

        // Register recovery callback to re-register consumer after connection recovery.
        // With topologyRecoveryEnabled=false, consumers are not automatically recovered
        // by the RabbitMQ client, so we must re-setup manually.
        connectionHolder.onConnectionEstablished(conn -> {
            try {
                // Close old channel (it has been recovered but has no consumer registered)
                Channel oldChannel = channelRef.get();
                if (oldChannel != null) {
                    try {
                        if (oldChannel.isOpen()) {
                            oldChannel.close();
                        }
                    } catch (Exception e) {
                        // Ignore - channel may already be closed
                    }
                }
                // Re-setup consumer on recovered connection
                setupConsumerOnConnection(onMessage, onError, onComplete);
            } catch (Exception e) {
                log.unableToCreateConsumer(configuration.getChannel(), e);
            }
        });

        connectionHolder.connect()
                .subscribe().with(
                        conn -> {
                            try {
                                setupConsumerOnConnection(onMessage, onError, onComplete);
                            } catch (Exception e) {
                                log.unableToCreateConsumer(configuration.getChannel(), e);
                                onError.accept(e);
                            }
                        },
                        error -> {
                            log.unableToCreateConsumer(configuration.getChannel(), error);
                            onError.accept(error);
                        });
    }

    private void setupConsumerOnConnection(
            java.util.function.Consumer<Message<?>> onMessage,
            java.util.function.Consumer<Throwable> onError,
            Runnable onComplete) throws Exception {

        // Create channel for consuming
        Channel channel = connectionHolder.createChannel();
        channelRef.set(channel);

        Context context = connectionHolder.getContext();

        // Set up QoS for backpressure (prefetch count)
        // Note: In auto-ack mode, messages are immediately acknowledged upon delivery,
        // but prefetch still controls how many messages are sent to the consumer at once
        if (maxOutstandingMessages > 0) {
            channel.basicQos(maxOutstandingMessages);
            log.qosSet(maxOutstandingMessages, configuration.getChannel());
        }

        // Declare topology
        setupTopology(channel);

        // Get queue name
        final String queueName = RabbitMQClientHelper.getQueueName(configuration);
        final String serverQueueName = RabbitMQClientHelper.serverQueueName(queueName);

        // Create acknowledgement handlers with outstanding message tracking built in
        RabbitMQAckHandler ackHandler;
        RabbitMQNackHandler nackHandler;

        if (autoAck) {
            ackHandler = RabbitMQAutoAck.INSTANCE;
            nackHandler = RabbitMQAutoAck.INSTANCE;
        } else {
            RabbitMQAck baseAck = new RabbitMQAck(channel, context);
            RabbitMQNack baseNack = new RabbitMQNack(channel, context, false);
            RabbitMQNackHandler failureNackHandler = createFailureNackHandler(baseAck, baseNack);
            ackHandler = new RabbitMQAckHandler() {
                @Override
                public <V> java.util.concurrent.CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message) {
                    return baseAck.handle(message)
                            .thenRun(outstandingMessages::decrementAndGet);
                }
            };
            nackHandler = new RabbitMQNackHandler() {
                @Override
                public <V> java.util.concurrent.CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message,
                        Metadata metadata, Throwable reason) {
                    return failureNackHandler.handle(message, metadata, reason)
                            .whenComplete((v, t) -> outstandingMessages.decrementAndGet());
                }
            };
        }

        // Create consumer
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                    AMQP.BasicProperties properties, byte[] body) throws IOException {

                // Track outstanding messages for backpressure
                if (!autoAck) {
                    outstandingMessages.incrementAndGet();
                }

                // Emit message (dispatch to Vert.x context is handled by emitOn in createStream)
                try {
                    // Convert to message - ack/nack handlers already include
                    // outstanding message counter tracking
                    String contentTypeOverride = configuration.getContentTypeOverride().orElse(null);
                    IncomingRabbitMQMessage<byte[]> message = IncomingRabbitMQMessage.create(
                            envelope,
                            properties,
                            body,
                            IncomingRabbitMQMessage.BYTE_ARRAY_CONVERTER,
                            ackHandler,
                            nackHandler,
                            context,
                            contentTypeOverride);

                    // Apply tracing if enabled
                    Message<?> tracedMessage = message;
                    if (configuration.getTracingEnabled() && instrumenter != null) {
                        tracedMessage = instrumenter.traceIncoming(message,
                                RabbitMQTrace.traceQueue(serverQueueName, envelope.getRoutingKey(),
                                        message.getRabbitMQMetadata().getHeaders()));
                    }

                    onMessage.accept(tracedMessage);
                } catch (Exception e) {
                    log.messageConversionFailed(configuration.getChannel(), e);
                    onError.accept(e);
                }
            }

            @Override
            public void handleCancel(String consumerTag) throws IOException {
                log.consumerCancelled(configuration.getChannel(), consumerTag);
                context.runOnContext(v -> onComplete.run());
            }

            @Override
            public void handleShutdownSignal(String consumerTag, com.rabbitmq.client.ShutdownSignalException sig) {
                if (!sig.isInitiatedByApplication()) {
                    // Log but don't terminate the stream - automatic recovery will handle reconnection
                    log.consumerShutdown(configuration.getChannel(), consumerTag, sig);
                }
            }
        };

        // Parse consumer arguments from config
        java.util.Map<String, Object> consumerArguments = parseConsumerArguments();
        String configuredConsumerTag = configuration.getConsumerTag().orElse("");
        boolean exclusive = configuration.getConsumerExclusive().orElse(false);

        // Start consuming
        String consumerTag;
        if (!consumerArguments.isEmpty() || !configuredConsumerTag.isEmpty() || exclusive) {
            consumerTag = channel.basicConsume(serverQueueName, autoAck,
                    configuredConsumerTag, false, exclusive, consumerArguments, consumer);
        } else {
            consumerTag = channel.basicConsume(serverQueueName, autoAck, consumer);
        }
        consumerTagRef.set(consumerTag);
        subscribed.set(true);

        log.consumerStarted(configuration.getChannel(), queueName, consumerTag);
    }

    private RabbitMQNackHandler createFailureNackHandler(RabbitMQAck baseAck, RabbitMQNack baseNack) {
        String failureStrategy = configuration.getFailureStrategy();
        String channelName = configuration.getChannel();

        switch (failureStrategy) {
            case RabbitMQFailureHandler.Strategy.ACCEPT:
                return new RabbitMQNackHandler() {
                    @Override
                    public <V> java.util.concurrent.CompletionStage<Void> handle(
                            IncomingRabbitMQMessage<V> message, Metadata metadata, Throwable reason) {
                        log.nackedAcceptMessage(channelName);
                        log.fullIgnoredFailure(reason);
                        return baseAck.handle(message);
                    }
                };
            case RabbitMQFailureHandler.Strategy.REJECT:
                return new RabbitMQNackHandler() {
                    @Override
                    public <V> java.util.concurrent.CompletionStage<Void> handle(
                            IncomingRabbitMQMessage<V> message, Metadata metadata, Throwable reason) {
                        log.nackedIgnoreMessage(channelName);
                        log.fullIgnoredFailure(reason);
                        // baseNack has defaultRequeue=false, so without RabbitMQRejectMetadata
                        // in the metadata it will reject without requeue. If the bean explicitly
                        // passes RabbitMQRejectMetadata, that override is respected.
                        return baseNack.handle(message, metadata, reason);
                    }
                };
            case RabbitMQFailureHandler.Strategy.REQUEUE:
                return new RabbitMQNackHandler() {
                    @Override
                    public <V> java.util.concurrent.CompletionStage<Void> handle(
                            IncomingRabbitMQMessage<V> message, Metadata metadata, Throwable reason) {
                        log.nackedIgnoreMessage(channelName);
                        log.fullIgnoredFailure(reason);
                        boolean requeue = Optional.ofNullable(metadata)
                                .flatMap(md -> md.get(RabbitMQRejectMetadata.class))
                                .map(RabbitMQRejectMetadata::isRequeue).orElse(true);
                        Metadata nackMetadata = metadata != null
                                ? metadata.with(new RabbitMQRejectMetadata(requeue))
                                : Metadata.of(new RabbitMQRejectMetadata(requeue));
                        return baseNack.handle(message, nackMetadata, reason);
                    }
                };
            case RabbitMQFailureHandler.Strategy.FAIL:
                return new RabbitMQNackHandler() {
                    @Override
                    public <V> java.util.concurrent.CompletionStage<Void> handle(
                            IncomingRabbitMQMessage<V> message, Metadata metadata, Throwable reason) {
                        log.nackedFailMessage(channelName);
                        return baseNack.handle(message, metadata, reason)
                                .thenCompose(v -> {
                                    CompletableFuture<Void> failed = new CompletableFuture<>();
                                    failed.completeExceptionally(reason);
                                    return failed;
                                });
                    }
                };
            default:
                throw ex.illegalArgumentUnknownFailureStrategy(failureStrategy);
        }
    }

    private void setupTopology(Channel channel) throws IOException {
        // Declare exchange if needed
        RabbitMQClientHelper.declareExchangeIfNeeded(channel, configuration, configMaps);

        // Declare queue if needed
        String queueName = RabbitMQClientHelper.declareQueueIfNeeded(channel, configuration, configMaps);

        // Establish bindings
        RabbitMQClientHelper.establishBindings(channel, configuration);

        // Configure DLQ/DLX if needed
        RabbitMQClientHelper.configureDLQorDLX(channel, configuration, configMaps);

        log.topologyEstablished(configuration.getChannel(), queueName);
    }

    /**
     * Parse consumer-arguments config into a Map.
     * Format: "key1:value1,key2:value2,..."
     * Values that look like integers are converted to Integer.
     */
    private java.util.Map<String, Object> parseConsumerArguments() {
        java.util.Map<String, Object> args = new java.util.HashMap<>();
        String consumerArgs = configuration.getConsumerArguments().orElse(null);
        if (consumerArgs != null && !consumerArgs.isEmpty()) {
            for (String pair : consumerArgs.split(",")) {
                String[] kv = pair.trim().split(":", 2);
                if (kv.length == 2) {
                    String key = kv[0].trim();
                    String value = kv[1].trim();
                    try {
                        args.put(key, Integer.parseInt(value));
                    } catch (NumberFormatException e) {
                        args.put(key, value);
                    }
                }
            }
        }
        return args;
    }

    /**
     * Check if the consumer is subscribed.
     */
    public boolean isSubscribed() {
        return subscribed.get();
    }

    /**
     * Get the number of outstanding (unacknowledged) messages.
     */
    public int getOutstandingMessages() {
        return outstandingMessages.get();
    }

    /**
     * Check if the channel is healthy.
     * Requires the consumer to be fully subscribed (topology declared and consumer registered).
     */
    public boolean isHealthy() {
        Channel channel = channelRef.get();
        return connectionHolder.isConnected() && channel != null && channel.isOpen() && subscribed.get();
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
        // If health-lazy-subscription is enabled and there's no subscription yet, report as healthy
        if (configuration.getHealthLazySubscription() && !subscribed.get()) {
            return builder.add(new io.smallrye.reactive.messaging.health.HealthReport.ChannelInfo(
                    configuration.getChannel(), true));
        }

        // Check if connection and channel are open
        boolean alive = isHealthy();
        return builder.add(new io.smallrye.reactive.messaging.health.HealthReport.ChannelInfo(
                configuration.getChannel(), alive));
    }

    /**
     * Cancel the consumer and clean up resources.
     */
    public void cancel() {
        subscribed.set(false);
        cleanup();
    }

    /**
     * Clean up resources.
     */
    private void cleanup() {
        try {
            Channel channel = channelRef.get();
            String consumerTag = consumerTagRef.get();

            if (channel != null && channel.isOpen() && consumerTag != null) {
                try {
                    channel.basicCancel(consumerTag);
                } catch (IOException e) {
                    log.unableToCancelConsumer(configuration.getChannel(), e);
                }
            }

            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (Exception e) {
                    log.unableToCloseChannel(configuration.getChannel(), e);
                }
            }

            subscribed.set(false);
        } catch (Exception e) {
            log.cleanupFailed(configuration.getChannel(), e);
        }
    }
}
