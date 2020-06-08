package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;

public interface KafkaFailureHandler {

    enum Strategy {
        FAIL,
        IGNORE,
        DEAD_LETTER_QUEUE;

        public static Strategy from(String s) {
            if (s == null || s.equalsIgnoreCase("fail")) {
                return FAIL;
            }
            if (s.equalsIgnoreCase("ignore")) {
                return IGNORE;
            }
            if (s.equalsIgnoreCase("dead-letter-queue")) {
                return DEAD_LETTER_QUEUE;
            }
            throw ex.illegalArgumentUnknownStrategy(s);
        }
    }

    <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason);

}
