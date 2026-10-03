package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.SerializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.health.KafkaSinkHealth;
import io.smallrye.reactive.messaging.kafka.impl.ce.KafkaCloudEventHelper;
import io.smallrye.reactive.messaging.kafka.reply.KafkaRequestReply;
import io.smallrye.reactive.messaging.kafka.tracing.KafkaOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.kafka.tracing.KafkaTrace;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.providers.helpers.SenderProcessor;

@SuppressWarnings("jol")
public class KafkaSink {

    private final KafkaProducer<?, ?> client;
    private final int partition;
    private final String topic;
    private final String key;
    private final Flow.Subscriber<? extends Message<?>> subscriber;

    private final long retries;
    private final int deliveryTimeoutMs;

    private final List<Throwable> failures = new ArrayList<>();
    private final SenderProcessor processor;
    private final boolean writeAsBinaryCloudEvent;
    private final boolean writeCloudEvents;
    private final boolean mandatoryCloudEventAttributeSet;
    private final boolean isTracingEnabled;
    private final KafkaSinkHealth health;
    private final boolean isHealthEnabled;
    private final boolean isHealthReadinessEnabled;
    private final String channel;

    private final RuntimeKafkaSinkConfiguration runtimeConfiguration;

    private final KafkaOpenTelemetryInstrumenter kafkaInstrumenter;

    public KafkaSink(KafkaConnectorOutgoingConfiguration config,
            KafkaCDIEvents kafkaCDIEvents,
            KafkaAdminClientRegistry adminClientRegistry,
            Instance<OpenTelemetry> openTelemetryInstance,
            Instance<ClientCustomizer<Map<String, Object>>> configCustomizers,
            Instance<SerializationFailureHandler<?>> serializationFailureHandlers,
            Instance<ProducerInterceptor<?, ?>> producerInterceptors) {
        this.isTracingEnabled = config.getTracingEnabled();
        this.partition = config.getPartition();
        this.retries = config.getSendRetries();
        this.topic = config.getTopic().orElseGet(config::getChannel);
        this.key = config.getKey().orElse(null);
        this.channel = config.getChannel();

        if (isPooledProducer(config)) {
            this.client = new PooledKafkaProducer<>(config, configCustomizers, serializationFailureHandlers,
                    producerInterceptors,
                    this::reportFailure,
                    (p, c) -> {
                        log.connectedToKafka(getClientId(c), config.getBootstrapServers(), topic);
                        kafkaCDIEvents.producer().fire(p);
                    });
        } else {
            this.client = new ReactiveKafkaProducer<>(config, configCustomizers, serializationFailureHandlers,
                    producerInterceptors,
                    this::reportFailure,
                    (p, c) -> {
                        log.connectedToKafka(getClientId(c), config.getBootstrapServers(), topic);
                        // fire producer event (e.g. bind metrics)
                        kafkaCDIEvents.producer().fire(p);
                    });
        }

        this.writeCloudEvents = config.getCloudEvents();
        this.writeAsBinaryCloudEvent = config.getCloudEventsMode().equalsIgnoreCase("binary");
        boolean waitForWriteCompletion = config.getWaitForWriteCompletion();
        this.mandatoryCloudEventAttributeSet = config.getCloudEventsType().isPresent()
                && config.getCloudEventsSource().isPresent();
        this.deliveryTimeoutMs = getDeliveryTimeoutMs(client.configuration());
        this.runtimeConfiguration = RuntimeKafkaSinkConfiguration.buildFromConfiguration(config);

        // Validate the serializer for structured Cloud Events
        if (config.getCloudEvents() &&
                config.getCloudEventsMode().equalsIgnoreCase("structured") &&
                !config.getValueSerializer().equalsIgnoreCase(StringSerializer.class.getName())) {
            log.invalidValueSerializerForStructuredCloudEvent(config.getValueSerializer());
            throw new IllegalStateException("Invalid value serializer to write a structured Cloud Event. "
                    + StringSerializer.class.getName() + " must be used, found: "
                    + config.getValueSerializer());
        }

        this.isHealthEnabled = config.getHealthEnabled();
        this.isHealthReadinessEnabled = config.getHealthReadinessEnabled();
        if (isHealthEnabled) {
            this.health = new KafkaSinkHealth(adminClientRegistry, config, client.configuration(), client);
        } else {
            this.health = null;
        }

        long requests = config.getMaxInflightMessages();
        if (requests <= 0) {
            requests = Long.MAX_VALUE;
        }
        this.processor = new SenderProcessor(requests, waitForWriteCompletion,
                writeMessageToKafka());
        this.subscriber = MultiUtils.via(processor, m -> m.onFailure().invoke(f -> {
            log.unableToDispatch(f);
            reportFailure(f);
        }));

        if (isTracingEnabled) {
            kafkaInstrumenter = KafkaOpenTelemetryInstrumenter.createForSink(openTelemetryInstance);
        } else {
            kafkaInstrumenter = null;
        }
    }

    private static boolean isPooledProducer(KafkaConnectorOutgoingConfiguration config) {
        return config.config().getOptionalValue("pooled-producer.enabled", Boolean.class)
                .orElseGet(() -> {
                    boolean pooled = config.getPooledProducer();
                    if (pooled) {
                        log.deprecatedConfig("pooled-producer", "pooled-producer.enabled");
                    }
                    return pooled;
                });
    }

    private static String getClientId(Map<String, Object> config) {
        return (String) config.get(ProducerConfig.CLIENT_ID_CONFIG);
    }

    private static int getDeliveryTimeoutMs(Map<String, ?> config) {
        int defaultDeliveryTimeoutMs = (Integer) ProducerConfig.configDef().defaultValues()
                .get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        String deliveryTimeoutString = (String) config.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        return deliveryTimeoutString != null ? Integer.parseInt(deliveryTimeoutString) : defaultDeliveryTimeoutMs;
    }

    private synchronized void reportFailure(Throwable failure) {
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);
    }

    private Function<Message<?>, Uni<Void>> writeMessageToKafka() {
        return message -> {
            Context spanContext = null;
            KafkaTrace kafkaTrace = null;
            try {
                OutgoingKafkaRecordMetadata<?> outgoingMetadata = message.getMetadata(OutgoingKafkaRecordMetadata.class)
                        .orElse(null);

                ProducerRecord<?, ?> record;
                OutgoingCloudEventMetadata<?> ceMetadata = message.getMetadata(OutgoingCloudEventMetadata.class)
                        .orElse(null);
                IncomingKafkaRecordMetadata<?, ?> incomingMetadata = message.getMetadata(IncomingKafkaRecordMetadata.class)
                        .orElse(null);
                String topic = getActualTopic(incomingMetadata, outgoingMetadata);

                if (message.getPayload() instanceof ProducerRecord) {
                    record = (ProducerRecord<?, ?>) message.getPayload();
                    topic = record.topic();
                } else if (writeCloudEvents && (ceMetadata != null || mandatoryCloudEventAttributeSet)) {
                    // We encode the outbound record as Cloud Events if:
                    // - cloud events are enabled -> writeCloudEvents
                    // - the incoming message contains Cloud Event metadata (OutgoingCloudEventMetadata -> ceMetadata)
                    // - or if the message does not contain this metadata, the type and source are configured on the channel
                    if (writeAsBinaryCloudEvent) {
                        record = KafkaCloudEventHelper.createBinaryRecord(message, topic, outgoingMetadata,
                                incomingMetadata,
                                ceMetadata, runtimeConfiguration);
                    } else {
                        record = KafkaCloudEventHelper
                                .createStructuredRecord(message, topic, outgoingMetadata, incomingMetadata, ceMetadata,
                                        runtimeConfiguration);
                    }
                } else {
                    record = getProducerRecord(message, outgoingMetadata, incomingMetadata, topic);
                }

                if (isTracingEnabled) {
                    kafkaTrace = new KafkaTrace.Builder()
                            .withPartition(record.partition() != null ? record.partition() : -1)
                            .withTopic(record.topic())
                            .withHeaders(record.headers())
                            .withClientId((String) client.configuration().get(ProducerConfig.CLIENT_ID_CONFIG))
                            .build();
                    spanContext = kafkaInstrumenter.startOutgoing(message, kafkaTrace);
                }

                String actualTopic = topic;
                log.sendingMessageToTopic(message, channel, actualTopic);

                // In pooled transaction mode, route sends through the transaction scope
                TransactionScopeMetadata scopeMeta = message.getMetadata(TransactionScopeMetadata.class).orElse(null);
                @SuppressWarnings({ "unchecked", "rawtypes" })
                Uni<RecordMetadata> sendUni = scopeMeta != null
                        ? scopeMeta.getScope().send((ProducerRecord) record)
                        : client.send((ProducerRecord) record);

                if (this.retries == Integer.MAX_VALUE) {
                    sendUni = sendUni.onFailure(this::isRecoverable).retry()
                            .withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(20)).expireIn(deliveryTimeoutMs);
                } else if (this.retries > 0) {
                    sendUni = sendUni.onFailure(this::isRecoverable).retry()
                            .withBackOff(Duration.ofSeconds(1), Duration.ofSeconds(20)).atMost(this.retries);
                }

                final Context finalSpanContext = spanContext;
                final KafkaTrace finalKafkaTrace = kafkaTrace;
                return sendUni.onItemOrFailure().transformToUni((recordMetadata, t) -> {
                    if (isTracingEnabled) {
                        kafkaInstrumenter.endOutgoing(finalSpanContext, finalKafkaTrace, recordMetadata, t);
                    }
                    if (t != null) {
                        log.nackingMessage(message, channel, actualTopic, t);
                        return Uni.createFrom().completionStage(message.nack(t));
                    } else {
                        OutgoingMessageMetadata.setResultOnMessage(message, recordMetadata);
                        log.successfullyToTopic(message, channel, recordMetadata.topic(), recordMetadata.partition(),
                                recordMetadata.offset());
                        return Uni.createFrom().completionStage(message.ack());
                    }
                });
            } catch (RuntimeException e) {
                if (isTracingEnabled && spanContext != null) {
                    kafkaInstrumenter.endOutgoing(spanContext, kafkaTrace, null, e);
                }
                log.unableToSendRecord(e);
                return Uni.createFrom().failure(e);
            }
        };
    }

    private String getActualTopic(IncomingKafkaRecordMetadata<?, ?> im, OutgoingKafkaRecordMetadata<?> om) {
        if (im != null) {
            Header replyTopic = im.getHeaders().lastHeader(KafkaRequestReply.DEFAULT_REPLY_TOPIC_HEADER);
            if (replyTopic != null) {
                return new String(replyTopic.value());
            }
        }
        return om == null || om.getTopic() == null ? this.topic : om.getTopic();
    }

    private boolean isRecoverable(Throwable f) {
        return f instanceof RetriableException && !client.isClosed();
    }

    @SuppressWarnings("rawtypes")
    private ProducerRecord<?, ?> getProducerRecord(Message<?> message, OutgoingKafkaRecordMetadata<?> om,
            IncomingKafkaRecordMetadata<?, ?> im, String actualTopic) {
        int actualPartition = getActualPartition(im, om);

        Object actualKey = getKey(message, om);

        long actualTimestamp;
        if ((om == null) || (om.getTimestamp() == null)) {
            actualTimestamp = -1;
        } else {
            actualTimestamp = (om.getTimestamp() != null) ? om.getTimestamp().toEpochMilli() : -1;
        }

        Headers kafkaHeaders = KafkaRecordHelper.getHeaders(om, im, runtimeConfiguration);
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

    private int getActualPartition(IncomingKafkaRecordMetadata<?, ?> im, OutgoingKafkaRecordMetadata<?> om) {
        if (im != null) {
            Header header = im.getHeaders().lastHeader(KafkaRequestReply.DEFAULT_REPLY_PARTITION_HEADER);
            if (header != null) {
                return KafkaRequestReply.replyPartitionFromBytes(header.value());
            }
        }
        return om == null || om.getPartition() <= -1 ? this.partition : om.getPartition();
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

        // Then, check if the message contains incoming metadata from which we can propagate the key
        if (runtimeConfiguration.getPropagateRecordKey()) {
            return message.getMetadata(IncomingKafkaRecordMetadata.class)
                    .map(IncomingKafkaRecordMetadata::getKey)
                    .orElse(key);
        }

        // Finally, check the configuration
        return key;
    }

    public Flow.Subscriber<? extends Message<?>> getSink() {
        return subscriber;
    }

    public void isAlive(HealthReport.HealthReportBuilder builder) {
        if (isHealthEnabled) {
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
        // If health is disabled, do not add anything to the builder.
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        // This method must not be called from the event loop.
        if (health != null && isHealthReadinessEnabled) {
            health.isReady(builder);
        }
        // If health is disabled, do not add anything to the builder.
    }

    public void isStarted(HealthReport.HealthReportBuilder builder) {
        // This method must not be called from the event loop.
        if (health != null) {
            health.isStarted(builder);
        }
        // If health is disabled, do not add anything to the builder.
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
        return channel;
    }

    public KafkaProducer<?, ?> getProducer() {
        return client;
    }
}
