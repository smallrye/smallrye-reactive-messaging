package io.smallrye.reactive.messaging.kafka.fault;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.producer.KafkaProducer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducerRecord;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class KafkaDeadLetterQueue implements KafkaFailureHandler {

    private final Logger logger;
    private final String channel;
    private final KafkaProducer producer;
    private final String topic;

    public KafkaDeadLetterQueue(Logger logger, String channel, String topic, KafkaProducer producer) {
        this.logger = logger;
        this.channel = channel;
        this.topic = topic;
        this.producer = producer;
    }

    public static KafkaFailureHandler create(Logger logger, Vertx vertx,
            Map<String, String> kafkaConfiguration, KafkaConnectorIncomingConfiguration conf) {
        Map<String, String> deadQueueProducerConfig = new HashMap<>(kafkaConfiguration);
        // TODO The producer may warn about consumer configuration - we may have to remove them.

        String keyDeserializer = deadQueueProducerConfig.remove("key.deserializer");
        String valueDeserializer = deadQueueProducerConfig.remove("value.deserializer");
        deadQueueProducerConfig.put("key.serializer",
                conf.getDeadLetterQueueKeySerializer().orElse(getMirrorSerializer(keyDeserializer)));
        deadQueueProducerConfig.put("value.serializer",
                conf.getDeadLetterQueueKeySerializer().orElse(getMirrorSerializer(valueDeserializer)));

        String deadQueueTopic = conf.getDeadLetterQueueTopic().orElse("dead-letter-topic-" + conf.getChannel());

        logger.debug("Dead queue letter configured with: topic: `{}`, key serializer: `{}`, value serializer: `{}`",
                deadQueueTopic,
                deadQueueProducerConfig.get("key.serializer"), deadQueueProducerConfig.get("value.serializer"));

        KafkaProducer<Object, Object> producer = io.vertx.mutiny.kafka.client.producer.KafkaProducer
                .create(vertx, deadQueueProducerConfig);

        return new KafkaDeadLetterQueue(logger, conf.getChannel(), deadQueueTopic, producer);

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
        logger
                .info("A message sent to channel `{}` has been nacked, sending the record to a dead letter topic {}",
                        channel, topic);
        return producer.send(dead)
                .subscribeAsCompletionStage();
    }
}
