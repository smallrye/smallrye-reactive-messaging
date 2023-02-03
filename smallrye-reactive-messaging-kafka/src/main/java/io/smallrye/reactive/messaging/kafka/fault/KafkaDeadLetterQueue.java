package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.impl.ConfigurationCleaner;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaProducer;
import io.vertx.mutiny.core.Vertx;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class KafkaDeadLetterQueue implements KafkaFailureHandler {

    public static final String DEAD_LETTER_EXCEPTION_CLASS_NAME = "dead-letter-exception-class-name";
    public static final String DEAD_LETTER_CAUSE_CLASS_NAME = "dead-letter-cause-class-name";

    public static final String DEAD_LETTER_REASON = "dead-letter-reason";
    public static final String DEAD_LETTER_CAUSE = "dead-letter-cause";
    public static final String DEAD_LETTER_TOPIC = "dead-letter-topic";
    public static final String DEAD_LETTER_OFFSET = "dead-letter-offset";
    public static final String DEAD_LETTER_PARTITION = "dead-letter-partition";

    private final String channel;
    private final KafkaProducer producer;
    private final String topic;
    private final BiConsumer<Throwable, Boolean> reportFailure;

    public KafkaDeadLetterQueue(String channel, String topic, KafkaProducer producer,
            BiConsumer<Throwable, Boolean> reportFailure) {
        this.channel = channel;
        this.topic = topic;
        this.producer = producer;
        this.reportFailure = reportFailure;
    }

    @ApplicationScoped
    @Identifier(Strategy.DEAD_LETTER_QUEUE)
    public static class Factory implements KafkaFailureHandler.Factory {

        @Inject
        KafkaCDIEvents kafkaCDIEvents;

        @Override
        public KafkaFailureHandler create(KafkaConnectorIncomingConfiguration config,
                Vertx vertx,
                KafkaConsumer<?, ?> consumer,
                BiConsumer<Throwable, Boolean> reportFailure) {
            Map<String, Object> deadQueueProducerConfig = new HashMap<>(consumer.configuration());
            String keyDeserializer = (String) deadQueueProducerConfig.remove(KEY_DESERIALIZER_CLASS_CONFIG);
            String valueDeserializer = (String) deadQueueProducerConfig.remove(VALUE_DESERIALIZER_CLASS_CONFIG);

            // We need to remove consumer interceptor
            deadQueueProducerConfig.remove(INTERCEPTOR_CLASSES_CONFIG);

            deadQueueProducerConfig.put(KEY_SERIALIZER_CLASS_CONFIG,
                    config.getDeadLetterQueueKeySerializer().orElse(getMirrorSerializer(keyDeserializer)));
            deadQueueProducerConfig.put(VALUE_SERIALIZER_CLASS_CONFIG,
                    config.getDeadLetterQueueValueSerializer().orElse(getMirrorSerializer(valueDeserializer)));
            deadQueueProducerConfig.put(CLIENT_ID_CONFIG,
                    config.getDeadLetterQueueProducerClientId()
                            .orElse("kafka-dead-letter-topic-producer-" + deadQueueProducerConfig.get(CLIENT_ID_CONFIG)));

            ConfigurationCleaner.cleanupProducerConfiguration(deadQueueProducerConfig);
            String deadQueueTopic = config.getDeadLetterQueueTopic().orElse("dead-letter-topic-" + config.getChannel());

            log.deadLetterConfig(deadQueueTopic,
                    (String) deadQueueProducerConfig.get(KEY_SERIALIZER_CLASS_CONFIG),
                    (String) deadQueueProducerConfig.get(VALUE_SERIALIZER_CLASS_CONFIG));

            // fire producer event (e.g. bind metrics)
            ReactiveKafkaProducer<Object, Object> producer = new ReactiveKafkaProducer<>(deadQueueProducerConfig,
                    deadQueueTopic, 10000, false, null, null, null,
                    (p, c) -> kafkaCDIEvents.producer().fire(p));

            return new KafkaDeadLetterQueue(config.getChannel(), deadQueueTopic, producer, reportFailure);
        }
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

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {

        OutgoingKafkaRecordMetadata<K> outgoing = metadata != null
                ? metadata.get(OutgoingKafkaRecordMetadata.class).orElse(null)
                : null;

        String topic = this.topic;
        if (outgoing != null && outgoing.getTopic() != null) {
            topic = outgoing.getTopic();
        }

        K key = record.getKey();
        if (outgoing != null && outgoing.getKey() != null) {
            key = outgoing.getKey();
        }

        Integer partition = null;
        if (outgoing != null && outgoing.getPartition() >= 0) {
            partition = outgoing.getPartition();
        }

        ProducerRecord<K, V> dead = new ProducerRecord<>(topic, partition, key, record.getPayload());

        addHeader(dead, DEAD_LETTER_EXCEPTION_CLASS_NAME, reason.getClass().getName());
        addHeader(dead, DEAD_LETTER_REASON, getThrowableMessage(reason));
        if (reason.getCause() != null) {
            addHeader(dead, DEAD_LETTER_CAUSE_CLASS_NAME, reason.getCause().getClass().getName());
            addHeader(dead, DEAD_LETTER_CAUSE, getThrowableMessage(reason.getCause()));
        }
        addHeader(dead, DEAD_LETTER_TOPIC, record.getTopic());
        addHeader(dead, DEAD_LETTER_PARTITION, Integer.toString(record.getPartition()));
        addHeader(dead, DEAD_LETTER_OFFSET, Long.toString(record.getOffset()));
        record.getHeaders().forEach(header -> dead.headers().add(header));
        if (outgoing != null && outgoing.getHeaders() != null) {
            outgoing.getHeaders().forEach(header -> dead.headers().add(header));
        }
        log.messageNackedDeadLetter(channel, topic);
        return producer.send(dead)
                .onFailure().invoke(t -> reportFailure.accept((Throwable) t, true))
                .onItem().ignore().andContinueWithNull()
                .chain(() -> Uni.createFrom().completionStage(record.ack()))
                .emitOn(record::runOnMessageContext);
    }

    void addHeader(ProducerRecord<?, ?> record, String key, String value) {
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void terminate() {
        producer.close();
    }
}
