package io.smallrye.reactive.messaging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

public class Messages {

    private Messages() {
        // Avoid direct instantiation.
    }

    public static <T> Message<T> merge(List<Message<?>> list, Function<List<?>, T> combinator) {
        if (list.isEmpty()) {
            return Message.of(combinator.apply(Collections.emptyList()));
        }

        T payload;
        try {
            payload = combinator.apply(list.stream().map(Message::getPayload).collect(Collectors.toList()));
        } catch (Exception e) {
            // Nack all the messages;
            list.forEach(m -> m.nack(e));
            throw e;
        }

        Supplier<CompletionStage<Void>> ack = () -> {
            List<CompletableFuture<Void>> acks = new ArrayList<>();
            for (Message<?> message : list) {
                acks.add(message.ack().toCompletableFuture());
            }
            return CompletableFuture.allOf(acks.toArray(new CompletableFuture[0]));
        };
        Function<Throwable, CompletionStage<Void>> nack = (e) -> {
            List<CompletableFuture<Void>> nacks = new ArrayList<>();
            for (Message<?> message : list) {
                nacks.add(message.nack(e).toCompletableFuture());
            }
            return CompletableFuture.allOf(nacks.toArray(new CompletableFuture[0]));
        };
        Metadata metadata = list.get(0).getMetadata();
        for (int i = 1; i < list.size(); i++) {
            Metadata other = list.get(1).getMetadata();
            metadata = merge(metadata, other);
        }

        return Message.of(payload)
                .withAck(ack)
                .withNack(nack)
                .withMetadata(metadata);
    }

    public static <T> Message<List<T>> merge(List<Message<T>> list) {
        if (list.isEmpty()) {
            return Message.of(Collections.emptyList());
        }
        List<T> payload = list.stream().map(Message::getPayload).collect(Collectors.toList());
        Supplier<CompletionStage<Void>> ack = () -> {
            List<CompletableFuture<Void>> acks = new ArrayList<>();
            for (Message<?> message : list) {
                acks.add(message.ack().toCompletableFuture());
            }
            return CompletableFuture.allOf(acks.toArray(new CompletableFuture[0]));
        };
        Function<Throwable, CompletionStage<Void>> nack = (e) -> {
            List<CompletableFuture<Void>> nacks = new ArrayList<>();
            for (Message<?> message : list) {
                nacks.add(message.nack(e).toCompletableFuture());
            }
            return CompletableFuture.allOf(nacks.toArray(new CompletableFuture[0]));
        };
        Metadata metadata = list.get(0).getMetadata();
        for (int i = 1; i < list.size(); i++) {
            Metadata other = list.get(i).getMetadata();
            metadata = merge(metadata, other);
        }

        return Message.of(payload)
                .withAck(ack)
                .withNack(nack)
                .withMetadata(metadata);
    }

    @SuppressWarnings("unchecked")
    private static Metadata merge(Metadata first, Metadata second) {
        Metadata result = first;
        for (Object meta : second) {
            Class<?> clazz = meta.getClass();
            Optional<?> value = result.get(clazz);
            // Do we already have a value?
            if (value.isEmpty()) {
                // No, just add the value from the second metadata
                result = result.with(meta);
            } else {
                // Yes, is it mergeable.
                if (MergeableMetadata.class.isAssignableFrom(clazz)) {
                    // Yes - merge
                    MergeableMetadata current = (MergeableMetadata) value.get();
                    Object merged = current.merge(meta);
                    if (merged != null) {
                        result = result.with(merged);
                    } else {
                        // null is an exception, it means that the value must not be added to the metadata
                        // at all.
                        result = result.without(clazz);
                    }
                } else {
                    // No, keep current one.
                }
            }
        }
        return result;
    }

}
