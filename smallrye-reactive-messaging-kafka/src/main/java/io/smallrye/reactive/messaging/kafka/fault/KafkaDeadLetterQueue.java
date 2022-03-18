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
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.impl.ConfigurationCleaner;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaProducer;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class KafkaDeadLetterQueue implements KafkaFailureHandler {

    public static final String DEAD_LETTER_REASON = "dead-letter-reason";
    public static final String DEAD_LETTER_CAUSE = "dead-letter-cause";
    public static final String DEAD_LETTER_TOPIC = "dead-letter-topic";
    public static final String DEAD_LETTER_OFFSET = "dead-letter-offset";
    public static final String DEAD_LETTER_PARTITION = "dead-letter-partition";

    private final String channel;
    private final ReactiveKafkaProducer producer;
    private final String topic;
    private final KafkaSource<?, ?> source;

    public KafkaDeadLetterQueue(String channel, String topic, ReactiveKafkaProducer producer, KafkaSource<?, ?> source) {
        this.channel = channel;
        this.topic = topic;
        this.producer = producer;
        this.source = source;
    }

    public static KafkaFailureHandler create(Map<String, ?> kafkaConfiguration,
            KafkaConnectorIncomingConfiguration conf, KafkaSource<?, ?> source, KafkaCDIEvents kafkaCDIEvents) {
        Map<String, String> deadQueueProducerConfig = new HashMap<>();
        kafkaConfiguration.forEach((key, value) -> deadQueueProducerConfig.put(key, (String) value));
        String keyDeserializer = deadQueueProducerConfig.remove(KEY_DESERIALIZER_CLASS_CONFIG);
        String valueDeserializer = deadQueueProducerConfig.remove(VALUE_DESERIALIZER_CLASS_CONFIG);

        // We need to remove consumer interceptor
        deadQueueProducerConfig.remove(INTERCEPTOR_CLASSES_CONFIG);

        deadQueueProducerConfig.put(KEY_SERIALIZER_CLASS_CONFIG,
                conf.getDeadLetterQueueKeySerializer().orElse(getMirrorSerializer(keyDeserializer)));
        deadQueueProducerConfig.put(VALUE_SERIALIZER_CLASS_CONFIG,
                conf.getDeadLetterQueueValueSerializer().orElse(getMirrorSerializer(valueDeserializer)));
        deadQueueProducerConfig.put(CLIENT_ID_CONFIG, "kafka-dead-letter-topic-producer-" + kafkaConfiguration.get(CLIENT_ID_CONFIG));

        ConfigurationCleaner.cleanupProducerConfiguration(deadQueueProducerConfig);
        String deadQueueTopic = conf.getDeadLetterQueueTopic().orElse("dead-letter-topic-" + conf.getChannel());

        log.deadLetterConfig(deadQueueTopic,
                deadQueueProducerConfig.get(KEY_SERIALIZER_CLASS_CONFIG),
                deadQueueProducerConfig.get(VALUE_SERIALIZER_CLASS_CONFIG));

        ReactiveKafkaProducer<Object, Object> producer = new ReactiveKafkaProducer(deadQueueProducerConfig,
                deadQueueTopic, 10000, null, null);

        // fire producer event (e.g. bind metrics)
        kafkaCDIEvents.producer().fire(producer.unwrap());

        return new KafkaDeadLetterQueue(conf.getChannel(), deadQueueTopic, producer, source);

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
    public <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {

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

        addHeader(dead, DEAD_LETTER_REASON, getThrowableMessage(reason));
        if (reason.getCause() != null) {
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
                .onFailure().invoke(t -> source.reportFailure((Throwable) t, true))
                .onItem().ignore().andContinueWithNull()
                .subscribeAsCompletionStage()
                .thenCompose(m -> record.ack());
    }

    void addHeader(ProducerRecord<?, ?> record, String key, String value) {
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void terminate() {
        producer.close();
    }
}
