package io.smallrye.reactive.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.CheckReturnValue;

/**
 * Utilities for handling coordination between messages.
 */
public interface Messages {

    /**
     * Chains the given message with some other messages.
     * It coordinates the acknowledgement. When all the other messages are acknowledged successfully, the passed
     * message is acknowledged. If one of the other messages is acknowledged negatively, the passed message is also
     * nacked (with the same reason). Subsequent ack/nack will be ignored.
     * <p>
     *
     * @param message the message
     * @return the chain builder that let you decide how the metadata are passed, and the set of messages.
     */
    @CheckReturnValue
    static MessageChainBuilder chain(Message<?> message) {
        return new MessageChainBuilder(message);
    }

    /**
     * Merges multiple messages into a single one.
     * This is an implementation of a <em>merge pattern</em>: n messages combined into 1.
     * <p>
     * Whe resulting message payload is computed using the combinator function.
     * When the returned message is acked/nacked, the passes messages are acked/nacked accordingly.
     * <p>
     * Metadata are also merged. The metadata of all the messages are copied into the resulting message. If, for a given
     * class, the metadata is already present in the result message, it's either ignored, or merged if the class
     * implements {@link MergeableMetadata}.
     *
     * @param list the list of message, must not be empty, must not be null
     * @param combinator the combinator method, must not be null
     * @param <T> the payload type of the produced message
     * @return the resulting message
     */
    static <T> Message<T> merge(List<Message<?>> list, Function<List<?>, T> combinator) {
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
            Metadata other = list.get(i).getMetadata();
            metadata = merge(metadata, other);
        }

        return Message.of(payload)
                .withAck(ack)
                .withNack(nack)
                .withMetadata(metadata);
    }

    /**
     * Merges multiple messages into a single one.
     * <p>
     * Whe resulting message payload is computed using the combinator function.
     * When the returned message is acked/nacked, the passes messages are acked/nacked accordingly.
     * <p>
     * Metadata are also merged. The metadata of all the messages are copied into the resulting message. If, for a given
     * class, the metadata is already present in the result message, it's either ignored, or merged if the class
     * implements {@link MergeableMetadata}.
     *
     * @param list the list of message, must not be empty, must not be null
     * @param <T> the payload type of the passed messages
     * @return the resulting message
     */
    static <T> Message<List<T>> merge(List<Message<T>> list) {
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
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

    /**
     * The message chain builder allows chaining message and configure metadata propagation.
     * By default, all the metadata from the given message are copied into the chained messages.
     */
    class MessageChainBuilder {
        private final Message<?> input;
        private Metadata metadata;

        private MessageChainBuilder(Message<?> message) {
            this.input = message;
            this.metadata = message.getMetadata().copy();
        }

        /**
         * Do not copy any metadata from the initial message to the chained message.
         *
         * @return the current {@link MessageChainBuilder}
         */
        @CheckReturnValue
        public MessageChainBuilder withoutMetadata() {
            this.metadata = Metadata.empty();
            return this;
        }

        /**
         * Copy the given metadata of the given classes from the initial message to the chained message, if the initial
         * message does not include a metadata object of the given class.
         *
         * In general, this method must be used after {@link #withoutMetadata()}.
         *
         * @return the current {@link MessageChainBuilder}
         */
        @CheckReturnValue
        public MessageChainBuilder withMetadata(Class<?>... mc) {
            for (Class<?> clazz : mc) {
                Optional<?> o = input.getMetadata().get(clazz);
                o.ifPresent(value -> this.metadata = metadata.with(value));
            }
            return this;
        }

        /**
         * Do not the given metadata of the given classes from the initial message to the chained message, if the initial
         * message does not include a metadata object of the given class.
         *
         * @return the current {@link MessageChainBuilder}
         */
        @CheckReturnValue
        public MessageChainBuilder withoutMetadata(Class<?>... mc) {
            for (Class<?> clazz : mc) {
                this.metadata = this.metadata.without(clazz);
            }
            return this;
        }

        /**
         * Chain the passed array of messages.
         * The messages are not modified, but should not be used afterward, and should be replaced by the messages contained
         * in the returned list.
         * This method preserve the order. So, the first message corresponds to the first message in the returned list.
         * The message from the returned list have the necessary logic to chain the ack/nack signals and the copied metadata.
         *
         * @param messages the chained messages, must not be empty, must not be null, must not contain null
         * @return the list of modified messages
         */
        public List<Message<?>> with(Message<?>... messages) {
            AtomicBoolean done = new AtomicBoolean();

            // Must be modifiable
            List<Message<?>> trackers = Arrays.stream(messages).collect(Collectors.toCollection(CopyOnWriteArrayList::new));
            List<Message<?>> outcomes = new ArrayList<>();
            for (Message<?> message : messages) {
                Message<?> tmp = message;
                for (Object metadatum : metadata) {
                    tmp = tmp.addMetadata(metadatum);
                }
                outcomes.add(tmp
                        .withAck(() -> {
                            CompletionStage<Void> acked = message.ack();
                            if (trackers.remove(message)) {
                                if (trackers.isEmpty() && done.compareAndSet(false, true)) {
                                    return acked.thenCompose(x -> input.ack());
                                }
                            }
                            return acked;
                        })
                        .withNack((reason) -> {
                            CompletionStage<Void> nacked = message.nack(reason);
                            if (trackers.remove(message)) {
                                if (done.compareAndSet(false, true)) {
                                    return nacked.thenCompose(x -> input.nack(reason));
                                }
                            }
                            return nacked;
                        }));
            }
            return outcomes;
        }

        /**
         * Chain the passed map of messages.
         * The messages are not modified, but should not be used afterward, and should be replaced by the messages
         * contained in the returned {@link TargetedMessages}.
         * Returned {@link TargetedMessages} keeps the same channel keys as the passed map.
         * Messages from the returned {@link TargetedMessages} have the necessary logic to chain the ack/nack signals
         * and the copied metadata.
         *
         * @param messages the map of channel name to message
         * @return the {@link TargetedMessages} containing modified messages with chained acknowledgement to the input
         */
        public TargetedMessages with(Map<String, Message<?>> messages) {
            AtomicBoolean done = new AtomicBoolean();
            // Must be modifiable
            List<Message<?>> trackers = new CopyOnWriteArrayList<>(messages.values());
            Map<String, Message<?>> outcomes = new HashMap<>();
            for (Map.Entry<String, Message<?>> entry : messages.entrySet()) {
                String key = entry.getKey();
                Message<?> message = entry.getValue();
                Message<?> tmp = message;
                for (Object metadatum : metadata) {
                    tmp = tmp.addMetadata(metadatum);
                }
                outcomes.put(key, tmp
                        .withAck(() -> {
                            CompletionStage<Void> acked = message.ack();
                            if (trackers.remove(message)) {
                                if (trackers.isEmpty() && done.compareAndSet(false, true)) {
                                    return acked.thenCompose(x -> input.ack());
                                }
                            }
                            return acked;
                        })
                        .withNack((reason) -> {
                            CompletionStage<Void> nacked = message.nack(reason);
                            if (trackers.remove(message)) {
                                if (done.compareAndSet(false, true)) {
                                    return nacked.thenCompose(x -> input.nack(reason));
                                }
                            }
                            return nacked;
                        }));
            }
            return TargetedMessages.from(outcomes);
        }
    }
}
