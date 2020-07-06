package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.serialization.StringSerializer;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.producer.KafkaProducer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducerRecord;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class KafkaDeadLetterQueue implements KafkaFailureHandler {

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
            Map<String, String> kafkaConfiguration, KafkaConnectorIncomingConfiguration conf, KafkaSource<?, ?> source) {
        Map<String, String> deadQueueProducerConfig = new HashMap<>(kafkaConfiguration);

        String keyDeserializer = deadQueueProducerConfig.remove("key.deserializer");
        String valueDeserializer = deadQueueProducerConfig.remove("value.deserializer");
        deadQueueProducerConfig.put("key.serializer",
                conf.getDeadLetterQueueKeySerializer().orElse(getMirrorSerializer(keyDeserializer)));
        deadQueueProducerConfig.put("value.serializer",
                conf.getDeadLetterQueueKeySerializer().orElse(getMirrorSerializer(valueDeserializer)));

        String deadQueueTopic = conf.getDeadLetterQueueTopic().orElse("dead-letter-topic-" + conf.getChannel());

        log.deadLetterConfig(deadQueueTopic,
                deadQueueProducerConfig.get("key.serializer"), deadQueueProducerConfig.get("value.serializer"));

        KafkaProducer<Object, Object> producer = io.vertx.mutiny.kafka.client.producer.KafkaProducer
                .create(vertx, deadQueueProducerConfig);

        return new KafkaDeadLetterQueue(conf.getChannel(), deadQueueTopic, producer, source);

    }

    private static String getMirrorSerializer(String deserializer) {
        if (deserializer == null) {
            return StringSerializer.class.getName();
        } else {
            return deserializer.replace("Deserializer", "Serializer");
        }
    }

    @Override
    public <K, V> CompletionStage<Void> handle(
            IncomingKafkaRecord<K, V> record, Throwable reason) {
        KafkaProducerRecord<K, V> dead = KafkaProducerRecord.create(topic, record.getKey(), record.getPayload());
        dead.addHeader("dead-letter-reason", reason.getMessage());
        if (reason.getCause() != null) {
            dead.addHeader("dead-letter-cause", reason.getCause().getMessage());
        }
        log.messageNackedDeadLetter(channel, topic);
        return producer.send(dead)
                .onFailure().invoke(t -> source.reportFailure((Throwable) t))
                .subscribeAsCompletionStage();
    }
}
