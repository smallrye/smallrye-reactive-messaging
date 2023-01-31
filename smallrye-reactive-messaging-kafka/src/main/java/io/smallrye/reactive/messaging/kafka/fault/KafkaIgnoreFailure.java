package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.vertx.mutiny.core.Vertx;

public class KafkaIgnoreFailure implements KafkaFailureHandler {

    private final String channel;

    @ApplicationScoped
    @Identifier(Strategy.IGNORE)
    public static class Factory implements KafkaFailureHandler.Factory {

        @Override
        public KafkaFailureHandler create(KafkaConnectorIncomingConfiguration config, Vertx vertx,
                KafkaConsumer<?, ?> consumer, BiConsumer<Throwable, Boolean> reportFailure) {
            return new KafkaIgnoreFailure(config.getChannel());
        }
    }

    public KafkaIgnoreFailure(String channel) {
        this.channel = channel;
    }

    @Override
    public <K, V> Uni<Void> handle(
            IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {
        // We commit the message, log and continue
        log.messageNackedIgnore(channel, reason.getMessage());
        log.messageNackedFullIgnored(reason);
        return Uni.createFrom().completionStage(record.ack());
    }
}
