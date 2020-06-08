package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;

public class KafkaIgnoreFailure implements KafkaFailureHandler {

    private final String channel;

    public KafkaIgnoreFailure(String channel) {
        this.channel = channel;
    }

    @Override
    public <K, V> CompletionStage<Void> handle(
            IncomingKafkaRecord<K, V> record, Throwable reason) {
        // We commit the message, log and continue
        log.messageNackedIgnore(channel, reason.getMessage());
        log.messageNackedFullIgnored(reason);
        return record.ack();
    }
}
