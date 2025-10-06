package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.providers.helpers.SenderProcessor;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarOpenTelemetryInstrumenter;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarTrace;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactionMetadata;

public class PulsarOutgoingChannel<T> {

    private final Producer<T> producer;
    private final SenderProcessor processor;
    private final Flow.Subscriber<? extends Message<?>> subscriber;
    private final String channel;
    private final boolean healthEnabled;
    private final List<Throwable> failures = new ArrayList<>();
    private final boolean tracingEnabled;
    private final PulsarOpenTelemetryInstrumenter instrumenter;

    public PulsarOutgoingChannel(PulsarClient client, Schema<T> schema, PulsarConnectorOutgoingConfiguration oc,
            ConfigResolver configResolver, Instance<OpenTelemetry> openTelemetryInstance) throws PulsarClientException {
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
        if (conf.getMaxPendingMessages() > 0 && conf.getMaxPendingMessagesAcrossPartitions() == 0) {
            conf.setMaxPendingMessagesAcrossPartitions(conf.getMaxPendingMessages());
        }
        ProducerBuilder<T> builder = configResolver.configure(client.newProducer(schema), oc, conf);
        this.producer = builder.create();
        log.createdProducerWithConfig(channel, SchemaResolver.getSchemaName(schema), configResolver.getConfig(builder));
        long requests = getRequests(oc, conf);

        processor = new SenderProcessor(requests, oc.getWaitForWriteCompletion(), this::sendMessage);
        subscriber = MultiUtils.via(processor, m -> m.onFailure().invoke(f -> {
            log.unableToDispatch(f);
            reportFailure(f);
        }));

        if (tracingEnabled) {
            instrumenter = PulsarOpenTelemetryInstrumenter.createForSink(openTelemetryInstance);
        } else {
            instrumenter = null;
        }
    }

    private long getRequests(PulsarConnectorOutgoingConfiguration oc, ProducerConfigurationData conf) {
        Optional<Integer> maxInflightMessages = oc.getMaxInflightMessages();
        if (maxInflightMessages.isPresent()) {
            if (maxInflightMessages.get() <= 0) {
                return Long.MAX_VALUE;
            } else {
                return maxInflightMessages.get();
            }
        } else if (producer.getNumOfPartitions() > 1 && conf.getMaxPendingMessagesAcrossPartitions() > 0) {
            return conf.getMaxPendingMessagesAcrossPartitions();
        } else {
            return 1000;
        }
    }

    private Uni<Void> sendMessage(Message<?> message) {
        return Uni.createFrom().item(message)
                .onItem().transform(m -> toMessageBuilder(m, producer))
                .onItem().transformToUni(mb -> Uni.createFrom().completionStage(mb::sendAsync))
                .onItemOrFailure().transformToUni((mid, t) -> {
                    if (t == null) {
                        OutgoingMessageMetadata.setResultOnMessage(message, mid);
                        return Uni.createFrom().completionStage(message.ack());
                    } else {
                        return Uni.createFrom().completionStage(message.nack(t));
                    }
                });
    }

    private TypedMessageBuilder<T> createMessageBuilder(Message<?> message, Transaction fallback) {
        Transaction transaction = message.getMetadata(PulsarTransactionMetadata.class)
                .map(PulsarTransactionMetadata::getTransaction)
                .orElse(fallback);
        return transaction != null ? producer.newMessage(transaction) : producer.newMessage();
    }

    private TypedMessageBuilder<T> toMessageBuilder(Message<?> message, Producer<T> producer) {
        Optional<PulsarOutgoingMessageMetadata> optionalMetadata = message.getMetadata(PulsarOutgoingMessageMetadata.class);
        final TypedMessageBuilder<T> messageBuilder;
        if (optionalMetadata.isPresent()) {
            PulsarOutgoingMessageMetadata metadata = optionalMetadata.get();
            Map<String, String> properties = metadata.getProperties();
            if (tracingEnabled) {
                PulsarTrace trace = new PulsarTrace.Builder()
                        .withProperties(properties)
                        .withSequenceId(metadata.getSequenceId())
                        .withTopic(producer.getTopic())
                        .build();
                properties = trace.getProperties();
                instrumenter.traceOutgoing(message, trace);
            }
            messageBuilder = createMessageBuilder(message, metadata.getTransaction());

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
                messageBuilder.properties(properties);
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
            messageBuilder = createMessageBuilder(message, null);
            if (tracingEnabled) {
                PulsarTrace trace = new PulsarTrace.Builder()
                        .withTopic(producer.getTopic())
                        .build();
                instrumenter.traceOutgoing(message, trace);
                messageBuilder.properties(trace.getProperties());
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
