package io.smallrye.reactive.messaging.pulsar.fault;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Consumer;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.pulsar.PulsarConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.pulsar.PulsarFailureHandler;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessage;
import io.smallrye.reactive.messaging.pulsar.PulsarReconsumeLaterMetadata;

/**
 * Failure strategy `reconsume-later` which calls reconsume later for the message
 */
public class PulsarReconsumeLater implements PulsarFailureHandler {
    public static final String STRATEGY_NAME = "reconsume-later";

    @ApplicationScoped
    @Identifier(STRATEGY_NAME)
    public static class Factory implements PulsarFailureHandler.Factory {

        @Override
        public PulsarFailureHandler create(Consumer<?> consumer, PulsarConnectorIncomingConfiguration config,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new PulsarReconsumeLater(consumer, config.getChannel(),
                    Duration.ofSeconds(config.getReconsumeLaterDelay()));
        }
    }

    private final Consumer<?> consumer;
    private final String channel;
    private final Duration defaultDelay;

    public PulsarReconsumeLater(Consumer<?> consumer, String channel, Duration defaultDelay) {
        this.consumer = consumer;
        this.channel = channel;
        this.defaultDelay = defaultDelay;
    }

    @Override
    public Uni<Void> handle(PulsarIncomingMessage<?> message, Throwable reason, Metadata metadata) {
        Optional<PulsarReconsumeLaterMetadata> reconsumeLater = Optional.ofNullable(metadata)
                .flatMap(m -> m.get(PulsarReconsumeLaterMetadata.class));
        final Duration delay = reconsumeLater.map(PulsarReconsumeLaterMetadata::getDelay)
                .orElse(this.defaultDelay);
        final Map<String, String> customProperties = reconsumeLater.map(PulsarReconsumeLaterMetadata::getCustomProperties)
                .orElse(null);

        log.messageFailureDelayed(channel, delay.toSeconds(), reason.getMessage());
        log.messageFailureFullCause(reason);
        return Uni.createFrom()
                .completionStage(
                        () -> consumer.reconsumeLaterAsync(message.unwrap(), customProperties, delay.toSeconds(), SECONDS))
                .emitOn(message::runOnMessageContext)
                .onItem().transformToUni(unused -> Uni.createFrom().completionStage(message.ack()))
                .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(message.nack(t)));
    }
}
