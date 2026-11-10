package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.pulsar.ack.PulsarCumulativeAck;
import io.smallrye.reactive.messaging.pulsar.ack.PulsarMessageAck;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarTrace;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mutiny.core.Vertx;

public class PulsarIncomingChannel<T> {

    private final Consumer<T> consumer;
    private final Flow.Publisher<? extends Message<?>> publisher;
    private final String channel;
    private final PulsarAckHandler ackHandler;
    /**
     * Ack handler for messages that failed decoding. Their failure strategy runs before earlier messages
     * have been processed, so a cumulative ack would also acknowledge those earlier messages.
     */
    private final PulsarAckHandler decodingFailureAckHandler;
    private final PulsarFailureHandler failureHandler;

    /**
     * This field captures the event loop context.
     * Using {@code ContextInternal} to distinguish it from {@code io.vertx.core.Context}.
     */
    private final ContextInternal context;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final List<Throwable> failures = new ArrayList<>();

    private final boolean healthEnabled;

    private final boolean tracingEnabled;

    private final PulsarOpenTelemetryInstrumenter instrumenter;

    public PulsarIncomingChannel(PulsarClient client, Vertx vertx, Schema<T> schema,
            PulsarAckHandler.Factory ackHandlerFactory,
            PulsarFailureHandler.Factory failureHandlerFactory,
            PulsarConnectorIncomingConfiguration ic,
            ConfigResolver configResolver,
            Instance<OpenTelemetry> openTelemetryInstance) throws PulsarClientException {
        this.channel = ic.getChannel();
        this.healthEnabled = ic.getHealthEnabled();
        this.tracingEnabled = ic.getTracingEnabled();
        ConsumerConfigurationData<?> conf = configResolver.getConsumerConf(ic);
        if (conf.getSubscriptionName() == null) {
            String s = UUID.randomUUID().toString();
            log.noSubscriptionName(s);
            conf.setSubscriptionName(s);
        }
        if (!hasTopicConfig(conf)) {
            conf.setTopicNames(Arrays.stream(ic.getTopic().orElse(channel).split(",")).collect(Collectors.toSet()));
        }
        if (conf.getConsumerName() == null) {
            conf.setConsumerName(channel);
        }
        ConsumerBuilder<T> builder = configResolver.configure(client.newConsumer(schema), ic, conf);
        this.consumer = builder.subscribe();
        log.createdConsumerWithConfig(channel, SchemaResolver.getSchemaName(schema), configResolver.getConfig(builder));
        this.ackHandler = ackHandlerFactory.create(consumer, ic);
        this.decodingFailureAckHandler = ackHandler instanceof PulsarCumulativeAck ? new PulsarMessageAck(consumer)
                : ackHandler;
        this.failureHandler = failureHandlerFactory.create(consumer, ic, this::reportFailure);
        this.context = ((VertxInternal) vertx.getDelegate()).createEventLoopContext();
        if (!ic.getBatchReceive()) {
            Multi<PulsarIncomingMessage<T>> receiveMulti = Multi.createBy().repeating()
                    .completionStage(consumer::receiveAsync)
                    .until(m -> closed.get())
                    // Decode before switching contexts: fetching schema information may block.
                    .onItem().transform(this::decode)
                    .emitOn(command -> context.runOnContext(event -> command.run()))
                    .onFailure(throwable -> isEndOfStream(client, throwable)).recoverWithCompletion()
                    .onFailure().invoke(failure -> {
                        log.failedToReceiveFromConsumer(channel, failure);
                        reportFailure(failure, false);
                    })
                    .onItem().transformToUniAndConcatenate(this::toIncoming);
            if (tracingEnabled) {
                receiveMulti = receiveMulti.onItem().invoke(this::incomingTrace);
            }
            this.publisher = receiveMulti;
        } else {
            Multi<PulsarIncomingBatchMessage<T>> batchReceiveMulti = Multi.createBy().repeating()
                    .completionStage(consumer::batchReceiveAsync)
                    .until(m -> closed.get())
                    .filter(m -> m.size() > 0)
                    .onItem().transform(this::decodeBatch)
                    .emitOn(command -> context.runOnContext(event -> command.run()))
                    .onFailure(throwable -> isEndOfStream(client, throwable)).recoverWithCompletion()
                    .onFailure().invoke(failure -> {
                        log.failedToReceiveFromConsumer(channel, failure);
                        reportFailure(failure, false);
                    })
                    .onItem().transformToUniAndConcatenate(this::toIncomingBatch);
            if (tracingEnabled) {
                batchReceiveMulti = batchReceiveMulti.onItem().invoke(this::incomingBatchTrace);
            }
            this.publisher = batchReceiveMulti;
        }

        if (tracingEnabled) {
            instrumenter = PulsarOpenTelemetryInstrumenter.createForSource(openTelemetryInstance);
        } else {
            instrumenter = null;
        }
    }

    private record DecodedMessage<T>(org.apache.pulsar.client.api.Message<T> message, T payload,
            SchemaSerializationException failure) {
    }

    private DecodedMessage<T> decode(org.apache.pulsar.client.api.Message<T> message) {
        try {
            return new DecodedMessage<>(message, message.getValue(), null);
        } catch (SchemaSerializationException failure) {
            return new DecodedMessage<>(message, null, failure);
        }
    }

    private List<DecodedMessage<T>> decodeBatch(Messages<T> messages) {
        List<DecodedMessage<T>> decoded = new ArrayList<>(messages.size());
        for (org.apache.pulsar.client.api.Message<T> message : messages) {
            decoded.add(decode(message));
        }
        return decoded;
    }

    private Uni<PulsarIncomingMessage<T>> toIncoming(DecodedMessage<T> decoded) {
        if (decoded.failure() == null) {
            return Uni.createFrom().item(new PulsarIncomingMessage<>(decoded.message(), ackHandler, failureHandler,
                    decoded.payload()));
        }
        PulsarIncomingMessage<T> failedMessage = new PulsarIncomingMessage<>(decoded.message(), decodingFailureAckHandler,
                failureHandler);
        // Await the configured strategy and omit the invalid message only if handling succeeds.
        return failureHandler.handle(failedMessage, decoded.failure(), failedMessage.getMetadata())
                .emitOn(command -> context.runOnContext(event -> command.run()))
                .onItem().transform(ignored -> (PulsarIncomingMessage<T>) null);
    }

    private Uni<PulsarIncomingBatchMessage<T>> toIncomingBatch(List<DecodedMessage<T>> decoded) {
        return Multi.createFrom().iterable(decoded)
                .onItem().transformToUniAndConcatenate(this::toIncoming)
                .collect().asList()
                .onItem().transform(messages -> messages.isEmpty() ? null : new PulsarIncomingBatchMessage<>(messages));
    }

    public void incomingTrace(PulsarMessage<T> pulsarMessage) {
        PulsarIncomingMessageMetadata metadata = pulsarMessage.getMetadata(PulsarIncomingMessageMetadata.class).get();
        instrumenter.traceIncoming(pulsarMessage, new PulsarTrace.Builder()
                .withConsumerName(consumer.getConsumerName())
                .withMessage(metadata.getMessage())
                .build());
    }

    public void incomingBatchTrace(PulsarIncomingBatchMessage<T> pulsarMessage) {
        for (PulsarMessage<T> message : pulsarMessage.getMessages()) {
            incomingTrace(message);
        }
    }

    private boolean isEndOfStream(PulsarClient client, Throwable throwable) {
        if (closed.get()) {
            return true;
        } else if (consumer.hasReachedEndOfTopic()) {
            log.consumerReachedEndOfTopic(channel);
            return true;
        } else if (client.isClosed()) {
            log.clientClosed(channel, throwable);
            return true;
        }
        return false;
    }

    static boolean hasTopicConfig(ConsumerConfigurationData<?> conf) {
        return conf.getTopicsPattern() != null
                || (conf.getTopicNames() != null && !conf.getTopicNames().isEmpty());
    }

    public Flow.Publisher<? extends Message<?>> getPublisher() {
        return publisher;
    }

    public String getChannel() {
        return channel;
    }

    public Consumer<T> getConsumer() {
        return consumer;
    }

    public void close() {
        closed.set(true);
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            log.unableToCloseConsumer(e);
        }
    }

    public synchronized void reportFailure(Throwable failure, boolean fatal) {
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);

        if (fatal) {
            close();
        }
    }

    public void isStarted(HealthReport.HealthReportBuilder builder) {
        if (healthEnabled) {
            builder.add(channel, consumer.isConnected());
        }
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        isStarted(builder);
    }

    public void isAlive(HealthReport.HealthReportBuilder builder) {
        if (healthEnabled) {
            List<Throwable> actualFailures;
            synchronized (this) {
                actualFailures = new ArrayList<>(failures);
            }
            if (!actualFailures.isEmpty()) {
                builder.add(channel, false,
                        actualFailures.stream().map(Throwable::getMessage).collect(Collectors.joining()));
            } else {
                builder.add(channel, true);
            }
        }

    }
}
