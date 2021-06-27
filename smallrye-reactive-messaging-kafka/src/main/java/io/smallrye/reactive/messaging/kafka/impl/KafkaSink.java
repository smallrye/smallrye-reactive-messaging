package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.TRACER;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.health.KafkaSinkReadinessHealth;
import io.smallrye.reactive.messaging.kafka.impl.ce.KafkaCloudEventHelper;
import io.smallrye.reactive.messaging.kafka.tracing.HeaderInjectAdapter;
import io.vertx.mutiny.core.Vertx;

public class KafkaSink {

    private final ReactiveKafkaProducer<?, ?> client;
    private final int partition;
    private final String topic;
    private final String key;
    private final SubscriberBuilder<? extends Message<?>, Void> subscriber;

    private final long retries;
    private final int deliveryTimeoutMs;

    private final KafkaConnectorOutgoingConfiguration configuration;
    private final List<Throwable> failures = new ArrayList<>();
    private final KafkaSenderProcessor processor;
    private final boolean writeAsBinaryCloudEvent;
    private final boolean writeCloudEvents;
    private final boolean mandatoryCloudEventAttributeSet;
    private final boolean isTracingEnabled;
    private final KafkaSinkReadinessHealth health;
    private final boolean isHealthEnabled;

    public KafkaSink(Vertx vertx, KafkaConnectorOutgoingConfiguration config, KafkaCDIEvents kafkaCDIEvents) {
        isTracingEnabled = config.getTracingEnabled();

        this.client = new ReactiveKafkaProducer<>(config);

        // fire producer event (e.g. bind metrics)
        kafkaCDIEvents.producer().fire(client.unwrap());

        partition = config.getPartition();
        retries = config.getRetries();
        int defaultDeliveryTimeoutMs = (Integer) ProducerConfig.configDef().defaultValues()
                .get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        String deliveryTimeoutString = client.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        deliveryTimeoutMs = deliveryTimeoutString != null ? Integer.parseInt(deliveryTimeoutString) : defaultDeliveryTimeoutMs;
        topic = config.getTopic().orElseGet(config::getChannel);
        key = config.getKey().orElse(null);
        writeCloudEvents = config.getCloudEvents();
        writeAsBinaryCloudEvent = config.getCloudEventsMode().equalsIgnoreCase("binary");
        boolean waitForWriteCompletion = config.getWaitForWriteCompletion();
        this.configuration = config;
        this.mandatoryCloudEventAttributeSet = configuration.getCloudEventsType().isPresent()
                && configuration.getCloudEventsSource().isPresent();

        // Validate the serializer for structured Cloud Events
        if (configuration.getCloudEvents() &&
                configuration.getCloudEventsMode().equalsIgnoreCase("structured") &&
                !configuration.getValueSerializer().equalsIgnoreCase(StringSerializer.class.getName())) {
            log.invalidValueSerializerForStructuredCloudEvent(configuration.getValueSerializer());
            throw new IllegalStateException("Invalid value serializer to write a structured Cloud Event. "
                    + StringSerializer.class.getName() + " must be used, found: "
                    + configuration.getValueSerializer());
        }

        this.isHealthEnabled = configuration.getHealthEnabled();
        if (isHealthEnabled && this.configuration.getHealthReadinessEnabled()) {
            this.health = new KafkaSinkReadinessHealth(vertx, config, client.configuration(), client.unwrap());
        } else {
            this.health = null;
        }

        long requests = config.getMaxInflightMessages();
        if (requests <= 0) {
            requests = Long.MAX_VALUE;
        }
        processor = new KafkaSenderProcessor(requests, waitForWriteCompletion,
                writeMessageToKafka());
        subscriber = ReactiveStreams.<Message<?>> builder()
                .via(processor)
                .onError(f -> {
                    log.unableToDispatch(f);
                    reportFailure(f);
                })
                .ignore();
    }

    private synchronized void reportFailure(Throwable failure) {
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);
    }

    /**
     * List exception for which we should not retry - they are fatal.
     * The list comes from https://kafka.apache.org/25/javadoc/org/apache/kafka/clients/producer/Callback.html.
     * <p>
     * Also included: SerializationException (as the chances to serialize the payload correctly one retry are almost 0).
     */
    private static final Set<Class<? extends Throwable>> NOT_RECOVERABLE = new HashSet<>(Arrays.asList(
            InvalidTopicException.class,
            OffsetMetadataTooLarge.class,
            RecordBatchTooLargeException.class,
            RecordTooLargeException.class,
            UnknownServerException.class,
            SerializationException.class));

    private Function<Message<?>, Uni<Void>> writeMessageToKafka() {
        return message -> {
            try {
                Optional<OutgoingKafkaRecordMetadata<?>> om = getOutgoingKafkaRecordMetadata(message);
                OutgoingKafkaRecordMetadata<?> metadata = om.orElse(null);
                String actualTopic = metadata == null || metadata.getTopic() == null ? this.topic : metadata.getTopic();

                ProducerRecord<?, ?> record;
                OutgoingCloudEventMetadata<?> ceMetadata = message.getMetadata(OutgoingCloudEventMetadata.class)
                        .orElse(null);

                if (message.getPayload() instanceof ProducerRecord) {
                    record = (ProducerRecord<?, ?>) message.getPayload();
                } else if (writeCloudEvents && (ceMetadata != null || mandatoryCloudEventAttributeSet)) {
                    // We encode the outbound record as Cloud Events if:
                    // - cloud events are enabled -> writeCloudEvents
                    // - the incoming message contains Cloud Event metadata (OutgoingCloudEventMetadata -> ceMetadata)
                    // - or if the message does not contain this metadata, the type and source are configured on the channel
                    if (writeAsBinaryCloudEvent) {
                        record = KafkaCloudEventHelper.createBinaryRecord(message, actualTopic, metadata, ceMetadata,
                                configuration);
                    } else {
                        record = KafkaCloudEventHelper
                                .createStructuredRecord(message, actualTopic, metadata, ceMetadata,
                                        configuration);
                    }
                } else {
                    record = getProducerRecord(message, metadata, actualTopic);
                }
                log.sendingMessageToTopic(message, actualTopic);

                @SuppressWarnings({ "unchecked", "rawtypes" })
                Uni<RecordMetadata> sendUni = client.send((ProducerRecord) record);

                Uni<Void> uni = sendUni.onItem().transformToUni(ignored -> {
                    log.successfullyToTopic(message, record.topic());
                    return Uni.createFrom().completionStage(message.ack());
                });

                if (this.retries == Integer.MAX_VALUE) {
                    uni = uni.onFailure(this::isRecoverable).retry()
                            .withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(20)).expireIn(deliveryTimeoutMs);
                } else if (this.retries > 0) {
                    uni = uni.onFailure(this::isRecoverable).retry()
                            .withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(20)).atMost(this.retries);
                }

                return uni
                        .onFailure().recoverWithUni(t -> {
                            // Log and nack the messages on failure.
                            log.nackingMessage(message, actualTopic, t);
                            return Uni.createFrom().completionStage(message.nack(t));
                        });
            } catch (RuntimeException e) {
                log.unableToSendRecord(e);
                return Uni.createFrom().failure(e);
            }
        };
    }

    private boolean isRecoverable(Throwable f) {
        return !NOT_RECOVERABLE.contains(f.getClass());
    }

    private Optional<OutgoingKafkaRecordMetadata<?>> getOutgoingKafkaRecordMetadata(Message<?> message) {
        Optional<OutgoingKafkaRecordMetadata<?>> metadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
                .map(x -> (OutgoingKafkaRecordMetadata<?>) x);
        if (metadata.isPresent()) {
            return metadata;
        }
        metadata = message.getMetadata(io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata.class)
                .map(x -> (OutgoingKafkaRecordMetadata<?>) x);
        return metadata;
    }

    @SuppressWarnings("rawtypes")
    private ProducerRecord<?, ?> getProducerRecord(Message<?> message, OutgoingKafkaRecordMetadata<?> om,
            String actualTopic) {
        int actualPartition = om == null || om.getPartition() <= -1 ? this.partition : om.getPartition();

        Object actualKey = getKey(message, om);

        long actualTimestamp;
        if ((om == null) || (om.getTimestamp() == null)) {
            actualTimestamp = -1;
        } else {
            actualTimestamp = (om.getTimestamp() != null) ? om.getTimestamp().toEpochMilli() : -1;
        }

        Headers kafkaHeaders = om == null || om.getHeaders() == null ? new RecordHeaders() : om.getHeaders();
        createOutgoingTrace(message, actualTopic, actualPartition, kafkaHeaders);
        Object payload = message.getPayload();
        if (payload instanceof Record) {
            payload = ((Record) payload).value();
        }

        return new ProducerRecord<>(
                actualTopic,
                actualPartition == -1 ? null : actualPartition,
                actualTimestamp == -1L ? null : actualTimestamp,
                actualKey,
                payload,
                kafkaHeaders);
    }

    @SuppressWarnings({ "rawtypes" })
    private Object getKey(Message<?> message,
            OutgoingKafkaRecordMetadata<?> metadata) {

        // First, the message metadata
        if (metadata != null && metadata.getKey() != null) {
            return metadata.getKey();
        }

        // Then, check if the message payload is a record
        if (message.getPayload() instanceof Record) {
            return ((Record) message.getPayload()).key();
        }

        // Finally, check the configuration
        return key;
    }

    private void createOutgoingTrace(Message<?> message, String topic, int partition, Headers headers) {
        if (isTracingEnabled) {
            Optional<TracingMetadata> tracingMetadata = TracingMetadata.fromMessage(message);

            final SpanBuilder spanBuilder = TRACER.spanBuilder(topic + " send")
                    .setSpanKind(SpanKind.PRODUCER);

            if (tracingMetadata.isPresent()) {
                // Handle possible parent span
                final Context parentSpanContext = tracingMetadata.get().getPreviousContext();
                if (parentSpanContext != null) {
                    spanBuilder.setParent(parentSpanContext);
                } else {
                    spanBuilder.setNoParent();
                }

                // Handle possible adjacent spans
                final SpanContext incomingSpan = tracingMetadata.get().getCurrentSpanContext();
                if (incomingSpan != null && incomingSpan.isValid()) {
                    spanBuilder.addLink(incomingSpan);
                }
            } else {
                spanBuilder.setNoParent();
            }

            final Span span = spanBuilder.startSpan();
            Scope scope = span.makeCurrent();

            // Set Span attributes
            if (partition != -1) {
                span.setAttribute(SemanticAttributes.MESSAGING_KAFKA_PARTITION, partition);
            }
            span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "kafka");
            span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, topic);
            span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, "topic");

            // Set span onto headers
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(Context.current(), headers, HeaderInjectAdapter.SETTER);
            span.end();
            scope.close();
        }
    }

    public SubscriberBuilder<? extends Message<?>, Void> getSink() {
        return subscriber;
    }

    public void isAlive(HealthReport.HealthReportBuilder builder) {
        if (isHealthEnabled) {
            List<Throwable> actualFailures;
            synchronized (this) {
                actualFailures = new ArrayList<>(failures);
            }
            if (!actualFailures.isEmpty()) {
                builder.add(configuration.getChannel(), false,
                        actualFailures.stream().map(Throwable::getMessage).collect(Collectors.joining()));
            } else {
                builder.add(configuration.getChannel(), true);
            }
        }
        // If health is disable do not add anything to the builder.
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        // This method must not be called from the event loop.
        if (health != null) {
            health.isReady(builder);
        }
        // If health is disable do not add anything to the builder.
    }

    public void closeQuietly() {
        if (processor != null) {
            processor.cancel();
        }

        try {
            this.client.close();
        } catch (Throwable e) {
            log.errorWhileClosingWriteStream(e);
        }

        if (health != null) {
            health.close();
        }
    }

    public String getChannel() {
        return configuration.getChannel();
    }

    public KafkaProducer<?, ?> getProducer() {
        return client;
    }
}
