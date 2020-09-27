package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.kafka.client.common.TopicPartition;

public interface KafkaCommitHandler {

    enum Strategy {
        LATEST,
        IGNORE,
        THROTTLED;

        public static KafkaCommitHandler.Strategy from(String s) {
            if (s.equalsIgnoreCase("latest")) {
                return LATEST;
            }
            if (s.equalsIgnoreCase("ignore")) {
                return IGNORE;
            }
            if (s.equalsIgnoreCase("throttled")) {
                return THROTTLED;
            }
            throw ex.illegalArgumentUnknownCommitStrategy(s);
        }

    }

    default <K, V> IncomingKafkaRecord<K, V> received(IncomingKafkaRecord<K, V> record) {
        return record;
    }

    default void terminate() {
        // Do nothing by default.
    }

    default void partitionsAssigned(Set<TopicPartition> partitions) {

    }

    <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record);

}
