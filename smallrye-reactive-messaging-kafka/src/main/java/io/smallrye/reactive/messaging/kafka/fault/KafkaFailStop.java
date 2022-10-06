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

public class KafkaFailStop implements KafkaFailureHandler {

    private final String channel;
    private final BiConsumer<Throwable, Boolean> reportFailure;

    @ApplicationScoped
    @Identifier(Strategy.FAIL)
    public static class Factory implements KafkaFailureHandler.Factory {

        @Override
        public KafkaFailureHandler create(KafkaConnectorIncomingConfiguration config, Vertx vertx, KafkaConsumer<?, ?> consumer,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new KafkaFailStop(config.getChannel(), reportFailure);
        }
    }

    public <K, V> KafkaFailStop(String channel, BiConsumer<Throwable, Boolean> reportFailure) {
        this.channel = channel;
        this.reportFailure = reportFailure;
    }

    @Override
    public <K, V> Uni<Void> handle(
            IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {
        // We don't commit, we just fail and stop the client.
        log.messageNackedFailStop(channel);
        // report failure to the connector for health check
        reportFailure.accept(reason, true);
        return Uni.createFrom().<Void> failure(reason)
                .emitOn(record::runOnMessageContext);
    }
}
