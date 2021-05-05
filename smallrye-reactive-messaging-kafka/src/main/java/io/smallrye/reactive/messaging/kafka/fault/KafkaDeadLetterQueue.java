package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.impl.ConfigurationCleaner;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.kafka.client.producer.KafkaHeader;
import io.vertx.mutiny.kafka.client.producer.KafkaProducer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducerRecord;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class KafkaDeadLetterQueue implements KafkaFailureHandler {

    public static final String DEAD_LETTER_REASON = "dead-letter-reason";
    public static final String DEAD_LETTER_CAUSE = "dead-letter-cause";
    public static final String DEAD_LETTER_TOPIC = "dead-letter-topic";
    public static final String DEAD_LETTER_OFFSET = "dead-letter-offset";
    public static final String DEAD_LETTER_PARTITION = "dead-letter-partition";

    private final String channel;
    private final KafkaProducer producer;
    private final String topic;
    private final KafkaSource<?, ?> source;

    public KafkaDeadLetterQueue(String channel, String topic, KafkaProducer producer, KafkaSource<?, ?> source) {
        this.channel = channel;
        this.topic = topic;
        this.producer = producer;
        this.source = source;
    }

    public static KafkaFailureHandler create(Vertx vertx,
            Map<String, ?> kafkaConfiguration, KafkaConnectorIncomingConfiguration conf, KafkaSource<?, ?> source,
            KafkaCDIEvents kafkaCDIEvents) {
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
        deadQueueProducerConfig.put(CLIENT_ID_CONFIG, "kafka-dead-letter-topic-producer-" + conf.getChannel());

        ConfigurationCleaner.cleanupProducerConfiguration(deadQueueProducerConfig);
        String deadQueueTopic = conf.getDeadLetterQueueTopic().orElse("dead-letter-topic-" + conf.getChannel());

        log.deadLetterConfig(deadQueueTopic,
                deadQueueProducerConfig.get(KEY_SERIALIZER_CLASS_CONFIG),
                deadQueueProducerConfig.get(VALUE_SERIALIZER_CLASS_CONFIG));

        KafkaProducer<Object, Object> producer = io.vertx.mutiny.kafka.client.producer.KafkaProducer
                .create(vertx, deadQueueProducerConfig);

        // fire producer event (e.g. bind metrics)
        kafkaCDIEvents.producer().fire(producer.getDelegate().unwrap());

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
    public <K, V> CompletionStage<Void> handle(
            IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {

        OutgoingKafkaRecordMetadata<K> outgoing = null;
        if (metadata != null) {
            for (Object item : metadata) {
                if (item instanceof OutgoingKafkaRecordMetadata) {
                    outgoing = (OutgoingKafkaRecordMetadata<K>) item;
                    // more than 1 instance => undefined behavior
                    break;
                }
            }
        }

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

        KafkaProducerRecord<K, V> dead = KafkaProducerRecord.create(topic, key, record.getPayload(), null, partition);

        dead.addHeader(DEAD_LETTER_REASON, getThrowableMessage(reason));
        if (reason.getCause() != null) {
            dead.addHeader(DEAD_LETTER_CAUSE, getThrowableMessage(reason.getCause()));
        }
        dead.addHeader(DEAD_LETTER_TOPIC, record.getTopic());
        dead.addHeader(DEAD_LETTER_PARTITION, Integer.toString(record.getPartition()));
        dead.addHeader(DEAD_LETTER_OFFSET, Long.toString(record.getOffset()));
        record.getHeaders().forEach(header -> dead.addHeader(KafkaHeader.header(header.key(), Buffer.buffer(header.value()))));
        if (outgoing != null && outgoing.getHeaders() != null) {
            outgoing.getHeaders()
                    .forEach(header -> dead.addHeader(KafkaHeader.header(header.key(), Buffer.buffer(header.value()))));
        }
        log.messageNackedDeadLetter(channel, topic);
        return producer.send(dead)
                .onFailure().invoke(t -> source.reportFailure((Throwable) t, true))
                .onItem().ignore().andContinueWithNull()
                .subscribeAsCompletionStage()
                .thenCompose(m -> record.ack());
    }

    @Override
    public void terminate() {
        producer.closeAndAwait();
    }
}
