package io.smallrye.reactive.messaging.kafka;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.kafka.impl.ConfigHelper;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.TopicPartitions;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(KafkaConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "bootstrap.servers", alias = "kafka.bootstrap.servers", type = "string", defaultValue = "localhost:9092", direction = Direction.INCOMING_AND_OUTGOING, description = "A comma-separated list of host:port to use for establishing the initial connection to the Kafka cluster.")
@ConnectorAttribute(name = "topic", type = "string", direction = Direction.INCOMING_AND_OUTGOING, description = "The consumed / populated Kafka topic. If neither this property nor the `topics` properties are set, the channel name is used")
@ConnectorAttribute(name = "health-enabled", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING, description = "Whether health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "health-readiness-enabled", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING, description = "Whether readiness health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "health-readiness-topic-verification", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING, description = "Whether the readiness check should verify that topics exist on the broker. Default to false. Enabling it requires an admin connection. Deprecated: Use 'health-topic-verification-enabled' instead.", deprecated = true)
@ConnectorAttribute(name = "health-readiness-timeout", type = "long", direction = Direction.INCOMING_AND_OUTGOING, description = "During the readiness health check, the connector connects to the broker and retrieves the list of topics. This attribute specifies the maximum duration (in ms) for the retrieval. If exceeded, the channel is considered not-ready. Deprecated: Use 'health-topic-verification-timeout' instead.", deprecated = true)
@ConnectorAttribute(name = "health-topic-verification-enabled", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING, description = "Whether the startup and readiness check should verify that topics exist on the broker. Default to false. Enabling it requires an admin client connection.", defaultValue = "false")
@ConnectorAttribute(name = "health-topic-verification-timeout", type = "long", direction = Direction.INCOMING_AND_OUTGOING, description = "During the startup and readiness health check, the connector connects to the broker and retrieves the list of topics. This attribute specifies the maximum duration (in ms) for the retrieval. If exceeded, the channel is considered not-ready.", defaultValue = "2000")

@ConnectorAttribute(name = "tracing-enabled", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "cloud-events", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING, description = "Enables (default) or disables the Cloud Event support. If enabled on an _incoming_ channel, the connector analyzes the incoming records and try to create Cloud Event metadata. If enabled on an _outgoing_, the connector sends the outgoing messages as Cloud Event if the message includes Cloud Event Metadata.", defaultValue = "true")
@ConnectorAttribute(name = "kafka-configuration", type = "string", direction = Direction.INCOMING_AND_OUTGOING, description = "Identifier of a CDI bean that provides the default Kafka consumer/producer configuration for this channel. The channel configuration can still override any attribute. The bean must have a type of Map<String, Object> and must use the @io.smallrye.common.annotation.Identifier qualifier to set the identifier.")
@ConnectorAttribute(name = "client-id-prefix", type = "string", direction = Direction.INCOMING_AND_OUTGOING, description = "Prefix for Kafka client `client.id` attribute. If defined configured or generated `client.id` will be prefixed with the given value.")
@ConnectorAttribute(name = "lazy-client", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING, description = "Whether Kafka client is created lazily or eagerly.", defaultValue = "false")

@ConnectorAttribute(name = "topics", type = "string", direction = Direction.INCOMING, description = "A comma-separating list of topics to be consumed. Cannot be used with the `topic` or `pattern` properties")
@ConnectorAttribute(name = "pattern", type = "boolean", direction = Direction.INCOMING, description = "Indicate that the `topic` property is a regular expression. Must be used with the `topic` property. Cannot be used with the `topics` property", defaultValue = "false")
@ConnectorAttribute(name = "key.deserializer", type = "string", direction = Direction.INCOMING, description = "The deserializer classname used to deserialize the record's key", defaultValue = "org.apache.kafka.common.serialization.StringDeserializer")
@ConnectorAttribute(name = "value.deserializer", type = "string", direction = Direction.INCOMING, description = "The deserializer classname used to deserialize the record's value", mandatory = true)
@ConnectorAttribute(name = "fetch.min.bytes", type = "int", direction = Direction.INCOMING, description = "The minimum amount of data the server should return for a fetch request. The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is available or the fetch request times out waiting for data to arrive.", defaultValue = "1")
@ConnectorAttribute(name = "group.id", type = "string", direction = Direction.INCOMING, description = "A unique string that identifies the consumer group the application belongs to. If not set, a unique, generated id is used")
@ConnectorAttribute(name = "enable.auto.commit", type = "boolean", direction = Direction.INCOMING, description = "If enabled, consumer's offset will be periodically committed in the background by the underlying Kafka client, ignoring the actual processing outcome of the records. It is recommended to NOT enable this setting and let Reactive Messaging handles the commit.", defaultValue = "false")
@ConnectorAttribute(name = "retry", type = "boolean", direction = Direction.INCOMING, description = "Whether or not the connection to the broker is re-attempted in case of failure", defaultValue = "true")
@ConnectorAttribute(name = "retry-attempts", type = "int", direction = Direction.INCOMING, description = "The maximum number of reconnection before failing. -1 means infinite retry", defaultValue = "-1")
@ConnectorAttribute(name = "retry-max-wait", type = "int", direction = Direction.INCOMING, description = "The max delay (in seconds) between 2 reconnects", defaultValue = "30")
@ConnectorAttribute(name = "broadcast", type = "boolean", direction = Direction.INCOMING, description = "Whether the Kafka records should be dispatched to multiple consumer", defaultValue = "false")
@ConnectorAttribute(name = "auto.offset.reset", type = "string", direction = Direction.INCOMING, description = "What to do when there is no initial offset in Kafka.Accepted values are earliest, latest and none", defaultValue = "latest")
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = Direction.INCOMING, description = "Specify the failure strategy to apply when a message produced from a record is acknowledged negatively (nack). Values can be `fail` (default), `ignore`, or `dead-letter-queue`", defaultValue = "fail")
@ConnectorAttribute(name = "commit-strategy", type = "string", direction = Direction.INCOMING, description = "Specify the commit strategy to apply when a message produced from a record is acknowledged. Values can be `latest`, `ignore` or `throttled`. If `enable.auto.commit` is true then the default is `ignore` otherwise it is `throttled`")
@ConnectorAttribute(name = "throttled.unprocessed-record-max-age.ms", type = "int", direction = Direction.INCOMING, description = "While using the `throttled` commit-strategy, specify the max age in milliseconds that an unprocessed message can be before the connector is marked as unhealthy. Setting this attribute to 0 disables this monitoring.", defaultValue = "60000")
@ConnectorAttribute(name = "checkpoint.state-store", type = "string", direction = Direction.INCOMING, description = "While using the `checkpoint` commit-strategy, the name set in `@Identifier` of a bean that implements `io.smallrye.reactive.messaging.kafka.StateStore.Factory` to specify the state store implementation.")
@ConnectorAttribute(name = "checkpoint.state-type", type = "string", direction = Direction.INCOMING, description = "While using the `checkpoint` commit-strategy, the fully qualified type name of the state object to persist in the state store. When provided, it can be used by the state store implementation to help persisting the processing state object.")
@ConnectorAttribute(name = "checkpoint.unsynced-state-max-age.ms", type = "int", direction = Direction.INCOMING, description = "While using the `checkpoint` commit-strategy, specify the max age in milliseconds that the processing state must be persisted before the connector is marked as unhealthy. Setting this attribute to 0 disables this monitoring.", defaultValue = "10000")
@ConnectorAttribute(name = "dead-letter-queue.topic", type = "string", direction = Direction.INCOMING, description = "When the `failure-strategy` is set to `dead-letter-queue` indicates on which topic the record is sent. Defaults is `dead-letter-topic-$channel`")
@ConnectorAttribute(name = "dead-letter-queue.producer-client-id", type = "string", direction = Direction.INCOMING, description = "When the `failure-strategy` is set to `dead-letter-queue` indicates what client id the generated producer should use. Defaults is `kafka-dead-letter-topic-producer-$client-id`")
@ConnectorAttribute(name = "dead-letter-queue.key.serializer", type = "string", direction = Direction.INCOMING, description = "When the `failure-strategy` is set to `dead-letter-queue` indicates the key serializer to use. If not set the serializer associated to the key deserializer is used")
@ConnectorAttribute(name = "dead-letter-queue.value.serializer", type = "string", direction = Direction.INCOMING, description = "When the `failure-strategy` is set to `dead-letter-queue` indicates the value serializer to use. If not set the serializer associated to the value deserializer is used")
@ConnectorAttribute(name = "partitions", type = "int", direction = Direction.INCOMING, description = "The number of partitions to be consumed concurrently. The connector creates the specified amount of Kafka consumers. It should match the number of partition of the targeted topic", defaultValue = "1")
@ConnectorAttribute(name = "requests", type = "int", direction = Direction.INCOMING, description = "When `partitions` is greater than 1, this attribute allows configuring how many records are requested by each consumers every time.", defaultValue = "128")
@ConnectorAttribute(name = "consumer-rebalance-listener.name", type = "string", direction = Direction.INCOMING, description = "The name set in `@Identifier` of a bean that implements `io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener`. If set, this rebalance listener is applied to the consumer.")
@ConnectorAttribute(name = "key-deserialization-failure-handler", type = "string", direction = Direction.INCOMING, description = "The name set in `@Identifier` of a bean that implements `io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler`. If set, deserialization failure happening when deserializing keys are delegated to this handler which may retry or provide a fallback value.")
@ConnectorAttribute(name = "value-deserialization-failure-handler", type = "string", direction = Direction.INCOMING, description = "The name set in `@Identifier` of a bean that implements `io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler`. If set, deserialization failure happening when deserializing values are delegated to this handler which may retry or provide a fallback value.")
@ConnectorAttribute(name = "fail-on-deserialization-failure", type = "boolean", direction = INCOMING, description = "When no deserialization failure handler is set and a deserialization failure happens, report the failure and mark the application as unhealthy. If set to `false` and a deserialization failure happens, a `null` value is forwarded.", defaultValue = "true")
@ConnectorAttribute(name = "graceful-shutdown", type = "boolean", direction = Direction.INCOMING, description = "Whether or not a graceful shutdown should be attempted when the application terminates.", defaultValue = "true")
@ConnectorAttribute(name = "poll-timeout", type = "int", direction = Direction.INCOMING, description = "The polling timeout in milliseconds. When polling records, the poll will wait at most that duration before returning records. Default is 1000ms", defaultValue = "1000")
@ConnectorAttribute(name = "pause-if-no-requests", type = "boolean", direction = Direction.INCOMING, description = "Whether the polling must be paused when the application does not request items and resume when it does. This allows implementing back-pressure based on the application capacity. Note that polling is not stopped, but will not retrieve any records when paused.", defaultValue = "true")
@ConnectorAttribute(name = "batch", type = "boolean", direction = Direction.INCOMING, description = "Whether the Kafka records are consumed in batch. The channel injection point must consume a compatible type, such as `List<Payload>` or `KafkaRecordBatch<Payload>`.", defaultValue = "false")
@ConnectorAttribute(name = "max-queue-size-factor", type = "int", direction = Direction.INCOMING, description = "Multiplier factor to determine maximum number of records queued for processing, using `max.poll.records` * `max-queue-size-factor`. Defaults to 2. In `batch` mode `max.poll.records` is considered `1`.", defaultValue = "2")

@ConnectorAttribute(name = "key.serializer", type = "string", direction = Direction.OUTGOING, description = "The serializer classname used to serialize the record's key", defaultValue = "org.apache.kafka.common.serialization.StringSerializer")
@ConnectorAttribute(name = "value.serializer", type = "string", direction = Direction.OUTGOING, description = "The serializer classname used to serialize the payload", mandatory = true)
@ConnectorAttribute(name = "acks", type = "string", direction = Direction.OUTGOING, description = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the durability of records that are sent. Accepted values are: 0, 1, all", defaultValue = "1")
@ConnectorAttribute(name = "buffer.memory", type = "long", direction = Direction.OUTGOING, description = "The total bytes of memory the producer can use to buffer records waiting to be sent to the server.", defaultValue = "33554432")
@ConnectorAttribute(name = "retries", type = "long", direction = Direction.OUTGOING, description = "If set to a positive number, the connector will try to resend any record that was not delivered successfully (with a potentially transient error) until the number of retries is reached. If set to 0, retries are disabled. If not set, the connector tries to resend any record that failed to be delivered (because of a potentially transient error) during an amount of time configured by `delivery.timeout.ms`.", defaultValue = "2147483647")
@ConnectorAttribute(name = "key", type = "string", direction = Direction.OUTGOING, description = "A key to used when writing the record")
@ConnectorAttribute(name = "partition", type = "int", direction = Direction.OUTGOING, description = "The target partition id. -1 to let the client determine the partition", defaultValue = "-1")
@ConnectorAttribute(name = "waitForWriteCompletion", type = "boolean", direction = Direction.OUTGOING, description = "Whether the client waits for Kafka to acknowledge the written record before acknowledging the message", defaultValue = "true")
@ConnectorAttribute(name = "max-inflight-messages", type = "long", direction = Direction.OUTGOING, description = "The maximum number of messages to be written to Kafka concurrently. It limits the number of messages waiting to be written and acknowledged by the broker. You can set this attribute to `0` remove the limit", defaultValue = "1024")
@ConnectorAttribute(name = "cloud-events-source", type = "string", direction = Direction.OUTGOING, description = "Configure the default `source` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `source` attribute itself", alias = "cloud-events-default-source")
@ConnectorAttribute(name = "cloud-events-type", type = "string", direction = Direction.OUTGOING, description = "Configure the default `type` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `type` attribute itself", alias = "cloud-events-default-type")
@ConnectorAttribute(name = "cloud-events-subject", type = "string", direction = Direction.OUTGOING, description = "Configure the default `subject` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `subject` attribute itself", alias = "cloud-events-default-subject")
@ConnectorAttribute(name = "cloud-events-data-content-type", type = "string", direction = Direction.OUTGOING, description = "Configure the default `datacontenttype` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `datacontenttype` attribute itself", alias = "cloud-events-default-data-content-type")
@ConnectorAttribute(name = "cloud-events-data-schema", type = "string", direction = Direction.OUTGOING, description = "Configure the default `dataschema` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `dataschema` attribute itself", alias = "cloud-events-default-data-schema")
@ConnectorAttribute(name = "cloud-events-insert-timestamp", type = "boolean", direction = Direction.OUTGOING, description = "Whether or not the connector should insert automatically the `time` attribute into the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `time` attribute itself", alias = "cloud-events-default-timestamp", defaultValue = "true")
@ConnectorAttribute(name = "cloud-events-mode", type = "string", direction = Direction.OUTGOING, description = "The Cloud Event mode (`structured` or `binary` (default)). Indicates how are written the cloud events in the outgoing record", defaultValue = "binary")
@ConnectorAttribute(name = "close-timeout", type = "int", direction = Direction.OUTGOING, description = "The amount of milliseconds waiting for a graceful shutdown of the Kafka producer", defaultValue = "10000")
@ConnectorAttribute(name = "merge", direction = Direction.OUTGOING, description = "Whether the connector should allow multiple upstreams", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "propagate-record-key", direction = Direction.OUTGOING, description = "Propagate incoming record key to the outgoing record", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "propagate-headers", direction = Direction.OUTGOING, description = "A comma-separating list of incoming record headers to be propagated to the outgoing record", type = "string", defaultValue = "")
@ConnectorAttribute(name = "key-serialization-failure-handler", type = "string", direction = Direction.OUTGOING, description = "The name set in `@Identifier` of a bean that implements `io.smallrye.reactive.messaging.kafka.SerializationFailureHandler`. If set, serialization failure happening when serializing keys are delegated to this handler which may provide a fallback value.")
@ConnectorAttribute(name = "value-serialization-failure-handler", type = "string", direction = Direction.OUTGOING, description = "The name set in `@Identifier` of a bean that implements `io.smallrye.reactive.messaging.kafka.SerializationFailureHandler`. If set, serialization failure happening when serializing values are delegated to this handler which may provide a fallback value.")
@ConnectorAttribute(name = "interceptor-bean", type = "string", direction = Direction.OUTGOING, description = "The name set in `@Identifier` of a bean that implements `org.apache.kafka.clients.producer.ProducerInterceptor`. If set, the identified bean will be used as the producer interceptor.")
public class KafkaConnector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-kafka";

    @Deprecated
    public static Tracer TRACER = new Tracer() {
        @Override
        public SpanBuilder spanBuilder(final String spanName) {
            throw new UnsupportedOperationException();
        }
    };

    @Inject
    ExecutionHolder executionHolder;

    @Inject
    @Any
    Instance<KafkaConsumerRebalanceListener> consumerRebalanceListeners;

    @Inject
    @Any
    Instance<ProducerInterceptor<?, ?>> producerInterceptors;

    @Inject
    @Any
    Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers;

    @Inject
    @Any
    Instance<SerializationFailureHandler<?>> serializationFailureHandlers;

    @Inject
    @Any
    Instance<KafkaCommitHandler.Factory> commitHandlerFactories;

    @Inject
    @Any
    Instance<KafkaFailureHandler.Factory> failureHandlerFactories;

    @Inject
    KafkaCDIEvents kafkaCDIEvents;

    private final List<KafkaSource<?, ?>> sources = new CopyOnWriteArrayList<>();
    private final List<KafkaSink> sinks = new CopyOnWriteArrayList<>();

    @Inject
    @Any
    Instance<Map<String, Object>> configurations;

    private Vertx vertx;

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        sources.forEach(KafkaSource::closeQuietly);
        sinks.forEach(KafkaSink::closeQuietly);
        TopicPartitions.clearCache();
    }

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        Config channelConfiguration = ConfigHelper.retrieveChannelConfiguration(configurations, config);

        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(channelConfiguration);
        // log deprecated config
        if (ic.getHealthReadinessTopicVerification().isPresent()) {
            log.deprecatedConfig("health-readiness-topic-verification", "health-topic-verification-enabled");
        }
        if (ic.getHealthReadinessTimeout().isPresent()) {
            log.deprecatedConfig("health-readiness-timeout", "health-topic-verification-timeout");
        }
        int partitions = ic.getPartitions();
        if (partitions <= 0) {
            throw new IllegalArgumentException("`partitions` must be greater than 0");
        }

        String group = ic.getGroupId().orElseGet(() -> {
            String s = UUID.randomUUID().toString();
            log.noGroupId(s);
            return s;
        });

        if (partitions == 1) {
            KafkaSource<Object, Object> source = new KafkaSource<>(vertx, group, ic,
                    commitHandlerFactories, failureHandlerFactories,
                    consumerRebalanceListeners,
                    kafkaCDIEvents, deserializationFailureHandlers, -1);
            sources.add(source);
            boolean broadcast = ic.getBroadcast();
            Multi<? extends Message<?>> stream;
            if (!ic.getBatch()) {
                stream = source.getStream();
            } else {
                stream = source.getBatchStream();
            }
            if (broadcast) {
                return stream.broadcast().toAllSubscribers();
            } else {
                return stream;
            }
        }

        // create an instance of source per partitions.
        List<Publisher<? extends Message<?>>> streams = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            KafkaSource<Object, Object> source = new KafkaSource<>(vertx, group, ic,
                    commitHandlerFactories, failureHandlerFactories,
                    consumerRebalanceListeners,
                    kafkaCDIEvents, deserializationFailureHandlers, i);
            sources.add(source);
            if (!ic.getBatch()) {
                streams.add(source.getStream());
            } else {
                streams.add(source.getBatchStream());
            }
        }

        @SuppressWarnings("unchecked")
        Multi<? extends Message<?>> multi = Multi.createBy().merging()
                .withRequests(ic.getRequests())
                .withConcurrency(partitions)
                .streams(streams.toArray(new Publisher[0]));
        boolean broadcast = ic.getBroadcast();
        if (broadcast) {
            return multi.broadcast().toAllSubscribers();
        } else {
            return multi;
        }
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        Config channelConfiguration = ConfigHelper.retrieveChannelConfiguration(configurations, config);

        KafkaConnectorOutgoingConfiguration oc = new KafkaConnectorOutgoingConfiguration(channelConfiguration);
        // log deprecated config
        if (oc.getHealthReadinessTopicVerification().isPresent()) {
            log.deprecatedConfig("health-readiness-topic-verification", "health-topic-verification-enabled");
        }
        if (oc.getHealthReadinessTimeout().isPresent()) {
            log.deprecatedConfig("health-readiness-timeout", "health-topic-verification-timeout");
        }
        KafkaSink sink = new KafkaSink(oc, kafkaCDIEvents, serializationFailureHandlers, producerInterceptors);
        sinks.add(sink);
        return sink.getSink();
    }

    @Override
    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (KafkaSource<?, ?> source : sources) {
            source.isStarted(builder);
        }
        for (KafkaSink sink : sinks) {
            sink.isStarted(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (KafkaSource<?, ?> source : sources) {
            source.isReady(builder);
        }
        for (KafkaSink sink : sinks) {
            sink.isReady(builder);
        }
        return builder.build();

    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (KafkaSource<?, ?> source : sources) {
            source.isAlive(builder);
        }
        for (KafkaSink sink : sinks) {
            sink.isAlive(builder);
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public <K, V> KafkaConsumer<K, V> getConsumer(String channel) {
        return (KafkaConsumer<K, V>) sources.stream()
                .filter(ks -> ks.getChannel().equals(channel))
                .map(KafkaSource::getConsumer)
                .findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    public <K, V> List<KafkaConsumer<K, V>> getConsumers(String channel) {
        return sources.stream()
                .filter(ks -> ks.getChannel().equals(channel))
                .map(kafkaSource -> ((KafkaConsumer<K, V>) kafkaSource.getConsumer()))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public <K, V> KafkaProducer<K, V> getProducer(String channel) {
        return (KafkaProducer<K, V>) sinks.stream()
                .filter(ks -> ks.getChannel().equals(channel))
                .map(KafkaSink::getProducer)
                .findFirst().orElse(null);
    }

    public Set<String> getConsumerChannels() {
        return sources.stream().map(KafkaSource::getChannel).collect(Collectors.toSet());
    }

    public Set<String> getProducerChannels() {
        return sinks.stream().map(KafkaSink::getChannel).collect(Collectors.toSet());
    }
}
