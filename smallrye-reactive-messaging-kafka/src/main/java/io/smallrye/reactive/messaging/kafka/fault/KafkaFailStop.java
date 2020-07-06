package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;

public class KafkaFailStop implements KafkaFailureHandler {

    private final String channel;
    private final KafkaSource<?, ?> source;

    public <K, V> KafkaFailStop(String channel, KafkaSource<?, ?> source) {
        this.channel = channel;
        this.source = source;
    }

    @Override
    public <K, V> CompletionStage<Void> handle(
            IncomingKafkaRecord<K, V> record, Throwable reason) {
        // We don't commit, we just fail and stop the client.
        log.messageNackedFailStop(channel);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(reason);
        // report failure to the connector for health check
        source.reportFailure(reason);
        return future;
    }
}
