package io.smallrye.reactive.messaging.helpers;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MediatorConfiguration;

public class MultiUtils {

    public static <T> Multi<T> createFromGenerator(Supplier<T> supplier) {
        return Multi.createFrom().items(Stream.generate(supplier));
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
                    if (ack != null) {
                        return Uni.createFrom().completionStage(ack).map(x -> message);
                    } else {
                        return Uni.createFrom().item(message);
                    }
                }));
    }

}
