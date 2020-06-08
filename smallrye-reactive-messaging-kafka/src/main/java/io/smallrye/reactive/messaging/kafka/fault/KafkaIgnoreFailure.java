package io.smallrye.reactive.messaging.kafka.fault;

import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;

public class KafkaIgnoreFailure implements KafkaFailureHandler {

    private final Logger logger;
    private final String channel;

    public KafkaIgnoreFailure(Logger logger, String channel) {
        this.logger = logger;
        this.channel = channel;
    }

    @Override
    public <K, V> CompletionStage<Void> handle(
            IncomingKafkaRecord<K, V> record, Throwable reason) {
        // We commit the message, log and continue
        logger.warn("A message sent to channel `{}` has been nacked, ignored failure is: {}.", channel,
                reason.getMessage());
        logger.debug("The full ignored failure is", reason);
        return record.ack();
    }
}
