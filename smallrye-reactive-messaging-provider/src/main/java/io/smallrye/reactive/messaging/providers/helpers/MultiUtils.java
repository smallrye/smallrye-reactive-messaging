package io.smallrye.reactive.messaging.providers.helpers;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.MediatorConfiguration;

public class MultiUtils {

    public static <T> Multi<T> createFromGenerator(Supplier<T> supplier) {
        return Multi.createFrom().generator(() -> null, (s, g) -> {
            g.emit(supplier.get());
            return s;
        });
    }

    public static <T> Multi<T> publisher(Publisher<T> publisher) {
        Publisher<T> actual = nonNull(publisher, "publisher");
        if (actual instanceof Multi) {
            return (Multi<T>) actual;
        }
        return Multi.createFrom().safePublisher(publisher);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Multi<? extends Message<?>> handlePreProcessingAcknowledgement(Multi<? extends Message<?>> multi,
            MediatorConfiguration configuration) {
        if (configuration.getAcknowledgment() != Acknowledgment.Strategy.PRE_PROCESSING) {
            return multi;
        }
        return multi.plug(stream -> (Multi) stream
                .onItem().transformToUniAndConcatenate(message -> {
                    CompletionStage<Void> ack = message.ack();
                    return Uni.createFrom().completionStage(ack).map(x -> message);
                }));
    }

}
