package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_DLQ;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.CHANNEL_DLQ_SUFFIX;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;
import static io.smallrye.reactive.messaging.providers.wiring.Wiring.wireOutgoingConnectorToUpstream;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.SerializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.commit.ContextHolder;
import io.smallrye.reactive.messaging.kafka.commit.KafkaLatestCommit;
import io.smallrye.reactive.messaging.kafka.impl.ConfigHelper;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;
import io.smallrye.reactive.messaging.providers.impl.Configs;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class KafkaDelayedRetryTopic extends ContextHolder implements KafkaFailureHandler {

    public static final String DELAYED_RETRY_TOPIC_STRATEGY = "delayed-retry-topic";

    public static final String DELAYED_RETRY_COUNT = "delayed-retry-count";
    public static final String DELAYED_RETRY_ORIGINAL_TIMESTAMP = "delayed-retry-original-timestamp";
    public static final String DELAYED_RETRY_FIRST_PROCESSING_TIMESTAMP = "delayed-retry-first-processing-timestamp";

    public static final String DELAYED_RETRY_EXCEPTION_CLASS_NAME = "delayed-retry-exception-class-name";
    public static final String DELAYED_RETRY_CAUSE_CLASS_NAME = "delayed-retry-cause-class-name";

    public static final String DELAYED_RETRY_REASON = "delayed-retry-reason";
    public static final String DELAYED_RETRY_CAUSE = "delayed-retry-cause";
    public static final String DELAYED_RETRY_TOPIC = "delayed-retry-topic";
    public static final String DELAYED_RETRY_OFFSET = "delayed-retry-offset";
    public static final String DELAYED_RETRY_PARTITION = "delayed-retry-partition";

    @ApplicationScoped
    @Identifier(DELAYED_RETRY_TOPIC_STRATEGY)
    public static class Factory implements KafkaFailureHandler.Factory {

        @Inject
        KafkaCDIEvents kafkaCDIEvents;

        @Inject
        @Any
        Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers;

        @Inject
        @Any
        Instance<ClientCustomizer<Map<String, Object>>> configCustomizers;

        @Inject
        @Any
        Instance<SerializationFailureHandler<?>> serializationFailureHandlers;

        @Inject
        @Any
        Instance<ProducerInterceptor<?, ?>> producerInterceptors;

        @Inject
        Instance<Config> rootConfig;

        @Inject
        @Any
        Instance<Map<String, Object>> configurations;

        @Inject
        Instance<OpenTelemetry> openTelemetryInstance;

        @Inject
        Instance<SubscriberDecorator> subscriberDecorators;

        @Override
        public KafkaFailureHandler create(KafkaConnectorIncomingConfiguration config,
                Vertx vertx,
                KafkaConsumer<?, ?> consumer,
                BiConsumer<Throwable, Boolean> reportFailure) {
            String keyDeserializer = (String) consumer.configuration().get(KEY_DESERIALIZER_CLASS_CONFIG);
            String valueDeserializer = (String) consumer.configuration().get(VALUE_DESERIALIZER_CLASS_CONFIG);

            List<String> retryTopics = config.getDelayedRetryTopicTopics()
                    .map(topics -> Arrays.stream(topics.split(",")).collect(Collectors.toList()))
                    .orElseGet(() -> Stream.of(
                            getRetryTopic(config.getChannel(), 10000),
                            getRetryTopic(config.getChannel(), 20000),
                            getRetryTopic(config.getChannel(), 50000)).collect(Collectors.toList()));
            int maxRetries = config.getDelayedRetryTopicMaxRetries().orElse(retryTopics.size());
            long retryTimeout = config.getDelayedRetryTopicTimeout();
            String deadQueueTopic = config.getDeadLetterQueueTopic().orElse(null);

            String consumerClientId = (String) consumer.configuration().get(CLIENT_ID_CONFIG);
            Config connectorConfig = Configs.prefixOverride(config.config(), CHANNEL_DLQ_SUFFIX,
                    Map.of(KEY_SERIALIZER_CLASS_CONFIG, c -> getMirrorSerializer(keyDeserializer),
                            VALUE_SERIALIZER_CLASS_CONFIG, c -> getMirrorSerializer(valueDeserializer),
                            CLIENT_ID_CONFIG, c -> config.getDeadLetterQueueProducerClientId()
                                    .orElse("kafka-delayed-retry-topic-producer-" + consumerClientId),
                            "topic", c -> retryTopics.get(0),
                            "key-serialization-failure-handler", c -> "dlq-serialization",
                            "value-serialization-failure-handler", c -> "dlq-serialization",
                            INTERCEPTOR_CLASSES_CONFIG, c -> ""));
            Config kafkaConfig = ConfigHelper.retrieveChannelConfiguration(configurations, connectorConfig);
            KafkaConnectorOutgoingConfiguration producerConfig = new KafkaConnectorOutgoingConfiguration(kafkaConfig);

            log.delayedRetryTopic(config.getChannel(), retryTopics, maxRetries, retryTimeout, deadQueueTopic);

            UnicastProcessor<Message<?>> processor = UnicastProcessor.create();
            KafkaSink kafkaSink = new KafkaSink(producerConfig, kafkaCDIEvents, openTelemetryInstance,
                    configCustomizers, serializationFailureHandlers, producerInterceptors);
            wireOutgoingConnectorToUpstream(processor, kafkaSink.getSink(), subscriberDecorators,
                    producerConfig.getChannel() + "-" + CHANNEL_DLQ_SUFFIX);

            Config retryConsumerConfig = Configs.prefixOverride(config.config(), "delayed-retry-topic.consumer",
                    Map.of("lazy-client", c -> true,
                            CLIENT_ID_CONFIG, c -> "kafka-delayed-retry-topic-" + consumerClientId,
                            GROUP_ID_CONFIG, c -> "kafka-delayed-retry-topic-" + consumerClientId));
            Config retryKafkaConfig = ConfigHelper.retrieveChannelConfiguration(configurations, retryConsumerConfig);
            KafkaConnectorIncomingConfiguration retryConfig = new KafkaConnectorIncomingConfiguration(retryKafkaConfig);

            ReactiveKafkaConsumer<Object, Object> retryConsumer = new ReactiveKafkaConsumer<>(retryConfig,
                    configCustomizers,
                    deserializationFailureHandlers,
                    retryConsumerConfig.getValue(GROUP_ID_CONFIG, String.class), -1,
                    reportFailure,
                    ((VertxInternal) vertx.getDelegate()).createEventLoopContext(),
                    c -> kafkaCDIEvents.consumer().fire(c));

            return new KafkaDelayedRetryTopic(config.getChannel(), vertx, config, retryTopics, maxRetries, retryTimeout,
                    deadQueueTopic, processor, kafkaSink, retryConsumer);
        }
    }

    private final String channel;
    private final Vertx vertx;
    private final KafkaConnectorIncomingConfiguration configuration;
    private final String deadQueueTopic;
    private final UnicastProcessor<Message<?>> dlqSource;
    private final KafkaSink dlqSink;
    private final ReactiveKafkaConsumer consumer;
    private final List<String> retryTopics;
    private final int maxRetries;
    private final long retryTimeout;

    public KafkaDelayedRetryTopic(String channel, Vertx vertx, KafkaConnectorIncomingConfiguration configuration,
            List<String> retryTopics,
            int maxRetries,
            long retryTimeout,
            String deadQueueTopic,
            UnicastProcessor<Message<?>> dlqSource,
            KafkaSink dlqSink,
            ReactiveKafkaConsumer consumer) {
        super(vertx, configuration.config()
                .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class).orElse(60000));
        this.channel = channel;
        this.vertx = vertx;
        this.configuration = configuration;
        this.retryTopics = retryTopics;
        this.maxRetries = maxRetries;
        this.retryTimeout = retryTimeout;
        this.deadQueueTopic = deadQueueTopic;
        this.dlqSource = dlqSource;
        this.dlqSink = dlqSink;
        this.consumer = consumer;
    }

    public static String getRetryTopic(String topic, int delayMillis) {
        return String.format("%s_retry_%d", topic, delayMillis);
    }

    private static String getMirrorSerializer(String deserializer) {
        if (deserializer == null) {
            return StringSerializer.class.getName();
        } else {
            return deserializer.replace("Deserializer", "Serializer");
        }
    }

    private String getThrowableMessage(Throwable throwable) {
        String text = throwable.getMessage();
        if (text == null) {
            text = throwable.toString();
        }
        return text;
    }

    public Multi<? extends IncomingKafkaRecord<?, ?>> retryStream() {
        KafkaLatestCommit latestCommit = new KafkaLatestCommit(vertx, configuration, consumer);
        consumer.setRebalanceListener(null, latestCommit);
        Multi<ConsumerRecord<?, ?>> subscribe = consumer.subscribe(new HashSet<>(retryTopics));
        latestCommit.capture(getContext());
        return subscribe.onItem().transform(record -> new IncomingKafkaRecord<>(record, channel, -1,
                latestCommit,
                this,
                configuration.getCloudEvents(),
                configuration.getTracingEnabled())).onItem().transformToUni(record -> {
                    incrementRetryHeader(record.getHeaders());
                    Duration between = getDelay(record);
                    if (between.isNegative()) {
                        return Uni.createFrom().item(record);
                    } else {
                        return Uni.createFrom().item(record).onItem().delayIt().by(between);
                    }
                }).concatenate(false);
    }

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {
        OutgoingKafkaRecordMetadata<K> outgoing = metadata != null
                ? metadata.get(OutgoingKafkaRecordMetadata.class).orElse(null)
                : null;
        setOriginalTimestampHeader(record);
        setFirstProcessingTimestampHeader(record);
        int retryCount = setAndGetRetryHeader(record.getHeaders());
        String topic = getNextTopic(this.retryTopics, this.deadQueueTopic, this.maxRetries, retryCount);
        if (!Objects.equals(topic, this.deadQueueTopic)) {
            int delayFromTopic = getDelayFromTopic(topic);
            if (retryWillTimeout(record, retryTimeout, delayFromTopic)) {
                log.delayedRetryTimeout(channel, retryTimeout, recordToString(record));
                topic = this.deadQueueTopic;
            }
        }
        if (outgoing != null && outgoing.getTopic() != null) {
            topic = outgoing.getTopic();
        }

        K key = record.getKey();
        if (outgoing != null && outgoing.getKey() != null) {
            key = outgoing.getKey();
        }

        int partition = record.getPartition();
        if (outgoing != null && outgoing.getPartition() >= 0) {
            partition = outgoing.getPartition();
        }

        if (topic == null) {
            log.delayedRetryNoDlq(channel);
            return Uni.createFrom().completionStage(record.ack())
                    .emitOn(record::runOnMessageContext);
        }

        ProducerRecord<K, V> retry = new ProducerRecord<>(topic, partition, key, record.getPayload());

        addHeader(retry, DELAYED_RETRY_EXCEPTION_CLASS_NAME, reason.getClass().getName());
        addHeader(retry, DELAYED_RETRY_REASON, getThrowableMessage(reason));
        if (reason.getCause() != null) {
            addHeader(retry, DELAYED_RETRY_CAUSE_CLASS_NAME, reason.getCause().getClass().getName());
            addHeader(retry, DELAYED_RETRY_CAUSE, getThrowableMessage(reason.getCause()));
        }
        addHeader(retry, DELAYED_RETRY_TOPIC, record.getTopic());
        addHeader(retry, DELAYED_RETRY_PARTITION, Integer.toString(record.getPartition()));
        addHeader(retry, DELAYED_RETRY_OFFSET, Long.toString(record.getOffset()));
        record.getHeaders().forEach(header -> retry.headers().add(header));
        if (outgoing != null && outgoing.getHeaders() != null) {
            outgoing.getHeaders().forEach(header -> retry.headers().add(header));
        }
        // remove DESERIALIZATION_FAILURE_DLQ header to prevent unconditional DQL in next consume
        retry.headers().remove(DESERIALIZATION_FAILURE_DLQ);
        log.delayedRetryNack(channel, topic);
        CompletableFuture<Void> future = new CompletableFuture<>();
        dlqSource.onNext(record.withPayload(retry)
                .withAck(() -> record.ack().thenAccept(__ -> future.complete(null)))
                .withNack(throwable -> {
                    future.completeExceptionally(throwable);
                    return future;
                }));
        return Uni.createFrom().completionStage(future)
                .emitOn(record::runOnMessageContext);
    }

    private boolean retryWillTimeout(IncomingKafkaRecord<?, ?> record, long retryTimeout, int delayFromTopic) {
        Instant nextRetry = Instant.now().plus(delayFromTopic, ChronoUnit.MILLIS);
        Instant firstProcessingTs = getFirstProcessingTimestamp(record);
        return Duration.between(firstProcessingTs, nextRetry).toMillis() > retryTimeout;
    }

    void addHeader(ProducerRecord<?, ?> record, String key, String value) {
        replaceHeader(record.headers(), key, value);
    }

    private static Instant getTimestampHeader(Headers headers, String key, long timestamp) {
        Header retry = headers.lastHeader(key);
        long epoch = retry == null ? timestamp : Long.parseLong(new String(retry.value()));
        return Instant.ofEpochMilli(epoch);
    }

    private static void setTimestampHeader(Headers headers, String key, long timestamp) {
        Header retry = headers.lastHeader(key);
        if (retry == null) {
            headers.add(key, Long.toString(timestamp).getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void terminate() {
        dlqSink.closeQuietly();
        consumer.close();
    }

    private static Duration getDelay(IncomingKafkaRecord<?, ?> retried) {
        int delay = getDelayFromTopic(retried.getTopic());
        return Duration.between(Instant.now(), retried.getTimestamp().plus(delay, ChronoUnit.MILLIS));
    }

    // visible for testing
    public static String getNextTopic(List<String> topics, String deadQueueTopic, int maxRetries, int retryCount) {
        int max = maxRetries <= 0 ? topics.size() : maxRetries;
        if (retryCount < max) {
            return topics.get(Math.min(retryCount, topics.size() - 1));
        } else {
            return deadQueueTopic;
        }
    }

    public static int getRetryHeader(Headers headers) {
        Header retry = headers.lastHeader(DELAYED_RETRY_COUNT);
        return retry == null ? 0 : Integer.parseInt(new String(retry.value()));
    }

    private static void incrementRetryHeader(Headers headers) {
        int count = getRetryHeader(headers) + 1;
        replaceHeader(headers, DELAYED_RETRY_COUNT, Integer.toString(count));
    }

    private static int setAndGetRetryHeader(Headers headers) {
        int count = getRetryHeader(headers);
        if (count == 0) {
            replaceHeader(headers, DELAYED_RETRY_COUNT, Integer.toString(count));
        }
        return count;
    }

    private static void setOriginalTimestampHeader(IncomingKafkaRecord<?, ?> record) {
        setTimestampHeader(record.getHeaders(), DELAYED_RETRY_ORIGINAL_TIMESTAMP, record.getTimestamp().toEpochMilli());
    }

    private static Instant getFirstProcessingTimestamp(IncomingKafkaRecord<?, ?> record) {
        return getTimestampHeader(record.getHeaders(), DELAYED_RETRY_FIRST_PROCESSING_TIMESTAMP, Instant.now().toEpochMilli());
    }

    private static void setFirstProcessingTimestampHeader(IncomingKafkaRecord<?, ?> record) {
        setTimestampHeader(record.getHeaders(), DELAYED_RETRY_FIRST_PROCESSING_TIMESTAMP, Instant.now().toEpochMilli());
    }

    private static int getDelayFromTopic(String topicName) {
        return Integer.parseInt(topicName.substring(topicName.lastIndexOf("_") + 1));
    }

    private static String recordToString(IncomingKafkaRecord<?, ?> record) {
        return String.format("%s-%d:%d", record.getTopic(), record.getPartition(), record.getOffset());
    }

    private static void replaceHeader(Headers headers, String key, String value) {
        headers.remove(key);
        if (value != null) {
            headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
