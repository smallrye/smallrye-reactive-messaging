package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarAttributesExtractor;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarTrace;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarTraceTextMapSetter;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactionMetadata;
import io.smallrye.reactive.messaging.tracing.TracingUtils;

public class PulsarOutgoingChannel<T> {

    private final Producer<T> producer;
    private final PulsarSenderProcessor processor;
    private final Flow.Subscriber<? extends Message<?>> subscriber;
    private final String channel;
    private final boolean healthEnabled;
    private final List<Throwable> failures = new ArrayList<>();
    private final boolean tracingEnabled;
    private final Instrumenter<PulsarTrace, Void> instrumenter;

    public PulsarOutgoingChannel(PulsarClient client, Schema<T> schema, PulsarConnectorOutgoingConfiguration oc,
            ConfigResolver configResolver) throws PulsarClientException {
        this.channel = oc.getChannel();
        this.healthEnabled = oc.getHealthEnabled();
        this.tracingEnabled = oc.getTracingEnabled();
        ProducerConfigurationData conf = configResolver.getProducerConf(oc);
        if (conf.getProducerName() == null) {
            conf.setProducerName(channel);
        }
        if (conf.getTopicName() == null) {
            conf.setTopicName(oc.getTopic().orElse(channel));
        }
        Map<String, Object> producerConf = configResolver.configToMap(conf);
        ProducerBuilder<T> builder = client.newProducer(schema)
                .loadConf(producerConf);
        if (conf.getBatcherBuilder() != null) {
            builder.batcherBuilder(conf.getBatcherBuilder());
        }
        if (conf.getCryptoKeyReader() != null) {
            builder.cryptoKeyReader(conf.getCryptoKeyReader());
        }
        for (String encryptionKey : conf.getEncryptionKeys()) {
            builder.addEncryptionKey(encryptionKey);
        }
        this.producer = builder.create();
        log.createdProducerWithConfig(channel, SchemaResolver.getSchemaName(schema), conf);
        long requests = oc.getMaxPendingMessages();
        if (requests <= 0) {
            requests = Long.MAX_VALUE;
        }

        processor = new PulsarSenderProcessor(requests, oc.getWaitForWriteCompletion(), this::sendMessage);
        subscriber = MultiUtils.via(processor, m -> m.onFailure().invoke(f -> {
            log.unableToDispatch(f);
            reportFailure(f);
        }));

        PulsarAttributesExtractor AttributesExtractor = new PulsarAttributesExtractor();
        MessagingAttributesGetter<PulsarTrace, Void> messagingAttributesGetter = AttributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<PulsarTrace, Void> instrumenterBuilder = Instrumenter.builder(GlobalOpenTelemetry.get(),
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, MessageOperation.SEND));

        instrumenter = instrumenterBuilder
                .addAttributesExtractor(MessagingAttributesExtractor.create(messagingAttributesGetter, MessageOperation.SEND))
                .addAttributesExtractor(AttributesExtractor)
                .buildProducerInstrumenter(PulsarTraceTextMapSetter.INSTANCE);
    }

    private Uni<Void> sendMessage(Message<?> message) {
        return Uni.createFrom().item(message)
                .onItem().transform(m -> toMessageBuilder(m, producer))
                .onItem().transformToUni(mb -> Uni.createFrom().completionStage(mb.sendAsync()))
                .onItemOrFailure().transformToUni((mid, t) -> {
                    if (t == null) {
                        OutgoingMessageMetadata.setResultOnMessage(message, mid);
                        return Uni.createFrom().completionStage(message.ack());
                    } else {
                        return Uni.createFrom().completionStage(message.nack(t));
                    }
                });
    }

    private TypedMessageBuilder<T> toMessageBuilder(Message<?> message, Producer<T> producer) {
        Optional<PulsarOutgoingMessageMetadata> optionalMetadata = message.getMetadata(PulsarOutgoingMessageMetadata.class);
        final TypedMessageBuilder<T> messageBuilder;
        if (optionalMetadata.isPresent()) {
            PulsarOutgoingMessageMetadata metadata = optionalMetadata.get();
            if (tracingEnabled) {
                TracingUtils.traceOutgoing(instrumenter, message, new PulsarTrace.Builder()
                        .withProperties(metadata.getProperties())
                        .withSequenceId(metadata.getSequenceId())
                        .withTopic(producer.getTopic())
                        .build());
            }
            Transaction transaction = message.getMetadata(PulsarTransactionMetadata.class)
                    .map(PulsarTransactionMetadata::getTransaction)
                    .orElse(metadata.getTransaction());
            messageBuilder = transaction != null ? producer.newMessage(transaction) : producer.newMessage();

            if (metadata.hasKey()) {
                if (metadata.getKeyBytes() != null) {
                    messageBuilder.keyBytes(metadata.getKeyBytes());
                } else {
                    messageBuilder.key(metadata.getKey());
                }
            }
            if (metadata.getOrderingKey() != null) {
                messageBuilder.orderingKey(metadata.getOrderingKey());
            }
            if (metadata.getProperties() != null) {
                messageBuilder.properties(metadata.getProperties());
            }
            if (metadata.getReplicatedClusters() != null) {
                messageBuilder.replicationClusters(metadata.getReplicatedClusters());
            }
            if (metadata.getReplicationDisabled() != null) {
                messageBuilder.disableReplication();
            }
            if (metadata.getEventTime() != null) {
                messageBuilder.eventTime(metadata.getEventTime());
            }
            if (metadata.getSequenceId() != null) {
                messageBuilder.sequenceId(metadata.getSequenceId());
            }
            if (metadata.getDeliverAt() != null) {
                messageBuilder.deliverAt(metadata.getDeliverAt());
            }
        } else {
            messageBuilder = producer.newMessage();
            if (tracingEnabled) {
                Map<String, String> properties = new HashMap<>();
                TracingUtils.traceOutgoing(instrumenter, message, new PulsarTrace.Builder()
                        .withProperties(properties)
                        .withTopic(producer.getTopic())
                        .build());
                messageBuilder.properties(properties);
            }
        }
        Object payload = message.getPayload();
        if (payload instanceof OutgoingMessage) {
            OutgoingMessage<?> outgoing = (OutgoingMessage<?>) payload;
            if (outgoing.hasKey()) {
                if (outgoing.getKeyBytes() != null) {
                    messageBuilder.keyBytes(outgoing.getKeyBytes());
                } else {
                    messageBuilder.key(outgoing.getKey());
                }
            }
            if (outgoing.getProperties() != null) {
                messageBuilder.properties(outgoing.getProperties());
            }
            if (outgoing.getOrderingKey() != null) {
                messageBuilder.orderingKey(outgoing.getOrderingKey());
            }
            if (outgoing.getSequenceId() != null) {
                messageBuilder.sequenceId(outgoing.getSequenceId());
            }
            if (outgoing.getEventTime() != null) {
                messageBuilder.eventTime(outgoing.getEventTime());
            }
            if (outgoing.getDeliverAt() != null) {
                messageBuilder.deliverAt(outgoing.getDeliverAt());
            }
            if (outgoing.getReplicationDisabled()) {
                messageBuilder.disableReplication();
            }
            if (outgoing.getReplicatedClusters() != null) {
                messageBuilder.replicationClusters(outgoing.getReplicatedClusters());
            }
            return messageBuilder.value((T) outgoing.getValue());
        } else {
            return messageBuilder.value((T) message.getPayload());
        }
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return subscriber;
    }

    public String getChannel() {
        return channel;
    }

    public Producer<T> getProducer() {
        return producer;
    }

    public void close() {
        if (processor != null) {
            processor.cancel();
        }
        try {
            producer.close();
        } catch (PulsarClientException e) {
            log.unableToCloseProducer(e);
        }
    }

    private synchronized void reportFailure(Throwable failure) {
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);
    }

    public void isStarted(HealthReport.HealthReportBuilder builder) {
        if (healthEnabled) {
            builder.add(channel, producer.isConnected());
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
