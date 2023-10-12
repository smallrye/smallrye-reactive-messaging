package io.smallrye.reactive.messaging.providers.extension;

import static io.smallrye.mutiny.unchecked.Unchecked.consumer;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.observation.MessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservationCollector;
import io.smallrye.reactive.messaging.observation.ObservationContext;
import io.smallrye.reactive.messaging.providers.ProcessingException;

@ApplicationScoped
public class ObservationDecorator implements PublisherDecorator {

    @Inject
    @ConfigProperty(name = "smallrye.messaging.observation.enabled", defaultValue = "true")
    boolean enabled;

    @Inject
    ChannelRegistry registry;

    @Inject
    Instance<MessageObservationCollector<?>> observationCollector;

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> multi, List<String> channelName,
            boolean isConnector) {
        String channel = channelName.isEmpty() ? null : channelName.get(0);
        boolean isEmitter = registry.getEmitterNames().contains(channel);
        if (observationCollector.isResolvable() && enabled && (isConnector || isEmitter)) {
            // if this is an emitter channel than it is an outgoing channel => incoming=false
            return decorateObservation(observationCollector.get(), multi, channel, !isEmitter, isEmitter);
        }
        return multi;
    }

    static Multi<? extends Message<?>> decorateObservation(
            MessageObservationCollector<? extends ObservationContext> obsCollector,
            Multi<? extends Message<?>> multi,
            String channel,
            boolean incoming,
            boolean emitter) {
        MessageObservationCollector<ObservationContext> collector = (MessageObservationCollector<ObservationContext>) obsCollector;
        ObservationContext context = collector.initObservation(channel, incoming, emitter);
        if (context == null) {
            return multi;
        }
        return multi.map(message -> {
            MessageObservation observation = collector.onNewMessage(channel, message, context);
            if (observation != null) {
                return message.addMetadata(observation)
                        .thenApply(msg -> msg.withAckWithMetadata(metadata -> msg.ack(metadata)
                                .thenAccept(consumer(x -> getObservationMetadata(metadata)
                                        .ifPresent(obs -> {
                                            obs.onMessageAck(msg);
                                            context.complete(obs);
                                        })))))
                        .thenApply(msg -> msg.withNackWithMetadata((reason, metadata) -> {
                            getObservationMetadata(metadata).ifPresent(consumer(obs -> {
                                obs.onMessageNack(msg, extractReason(reason));
                                context.complete(obs);
                            }));
                            return msg.nack(reason, metadata);
                        }));
            } else {
                return message;
            }
        });
    }

    static Optional<MessageObservation> getObservationMetadata(Metadata metadata) {
        for (Object item : metadata) {
            if (item instanceof MessageObservation) {
                return Optional.of((MessageObservation) item);
            }
        }
        return Optional.empty();
    }

    static Throwable extractReason(Throwable reason) {
        if (reason instanceof ProcessingException) {
            Throwable cause = reason.getCause();
            if (cause instanceof InvocationTargetException) {
                cause = ((InvocationTargetException) cause).getTargetException();
            }
            return cause;
        }
        return reason;
    }

}
