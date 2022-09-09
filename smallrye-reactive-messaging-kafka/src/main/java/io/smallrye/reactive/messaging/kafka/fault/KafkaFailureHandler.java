package io.smallrye.reactive.messaging.kafka.fault;

import java.util.function.BiConsumer;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.vertx.mutiny.core.Vertx;

/**
 * Kafka Failure handling strategy
 */
@Experimental("Experimental API")
public interface KafkaFailureHandler {

    /**
     * Identifiers of default failure strategies
     */
    interface Strategy {
        String FAIL = "fail";
        String IGNORE = "ignore";
        String DEAD_LETTER_QUEUE = "dead-letter-queue";

    }

    /**
     * Factory interface for {@link KafkaFailureHandler}
     */
    interface Factory {
        KafkaFailureHandler create(
                KafkaConnectorIncomingConfiguration config,
                Vertx vertx,
                KafkaConsumer<?, ?> consumer,
                BiConsumer<Throwable, Boolean> reportFailure);
    }

    /**
     * Handle message negative-acknowledgment
     *
     * @param record incoming Kafka record
     * @param reason nack reason
     * @param metadata associated metadata with negative-acknowledgment
     * @param <K> type of record key
     * @param <V> type of record value
     * @return a completion stage completed when the message is negative-acknowledgement has completed.
     */
    <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata);

    /**
     * Called on channel shutdown
     */
    default void terminate() {
        // do nothing by default
    }

}
