package io.smallrye.reactive.messaging.helpers;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class MultiUtils {


    public static <T> Multi<T> createFromGenerator(Supplier<T> supplier) {
        return Multi.createFrom().items(Stream.generate(supplier));
    }

    @SuppressWarnings("unchecked")
    public static Multi<? extends Message<?>> handlePreProcessingAcknowledgement(Multi<? extends Message<?>> multi, MediatorConfiguration configuration) {
        return multi.plug(stream -> {
            if (configuration.getAcknowledgment() == Acknowledgment.Strategy.PRE_PROCESSING) {
                return (Multi) stream
                        .onItem().transformToUniAndConcatenate(message -> {
                            CompletionStage<Void> ack = message.ack();
                            if (ack != null) {
                                return Uni.createFrom().completionStage(ack).map(x -> message);
                            } else {
                                return Uni.createFrom().item(message);
                            }
                        });
            }
            return stream;
        });
    }

}
