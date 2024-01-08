/**
 * Copyright (c) 2018-2019 Contributors to the Eclipse Foundation
 * <p>
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eclipse.microprofile.reactive.messaging;

import static java.util.Objects.requireNonNullElse;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

import io.smallrye.common.annotation.Experimental;

/**
 * A message envelope.
 * <p>
 * A message contains a non-{@code null} payload, an acknowledgement function and a set of metadata.
 * Metadata are indexed using the class name of the values.
 * </p>
 *
 * @param <T> The type of the message payload.
 */
public interface Message<T> {

    Logger LOGGER = Logger.getLogger(Message.class.getName());

    Function<Metadata, CompletionStage<Void>> EMPTY_ACK = m -> CompletableFuture.completedFuture(null);
    BiFunction<Throwable, Metadata, CompletionStage<Void>> EMPTY_NACK = (t, m) -> CompletableFuture.completedFuture(null);

    private static Function<Metadata, CompletionStage<Void>> validateAck(Function<Metadata, CompletionStage<Void>> ackM) {
        return (ackM != null) ? ackM : EMPTY_ACK;
    }

    private static Function<Metadata, CompletionStage<Void>> validateAck(Supplier<CompletionStage<Void>> ack) {
        return (ack != null) ? m -> ack.get() : EMPTY_ACK;
    }

    private static BiFunction<Throwable, Metadata, CompletionStage<Void>> validateNack(
            BiFunction<Throwable, Metadata, CompletionStage<Void>> nackM) {
        return (nackM != null) ? nackM : EMPTY_NACK;
    }

    private static BiFunction<Throwable, Metadata, CompletionStage<Void>> validateNack(
            Function<Throwable, CompletionStage<Void>> nack) {
        return (nack != null) ? (t, m) -> nack.apply(t) : EMPTY_NACK;
    }

    private static Function<Metadata, CompletionStage<Void>> wrapAck(Message<?> message) {
        var ackM = message.getAckWithMetadata();
        return ackM != null ? ackM : validateAck(message.getAck());
    }

    private static BiFunction<Throwable, Metadata, CompletionStage<Void>> wrapNack(Message<?> message) {
        var nackM = message.getNackWithMetadata();
        return nackM != null ? nackM : validateNack(message.getNack());
    }

    private static <T> Message<T> newMessage(T payload, Metadata metadata) {
        return new Message<>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return metadata;
            }
        };
    }

    private static <T> Message<T> newMessage(T payload,
            Function<Metadata, CompletionStage<Void>> actualAck) {
        return new Message<>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
                return actualAck;
            }

        };
    }

    private static <T> Message<T> newMessage(T payload,
            Metadata metadata,
            Function<Metadata, CompletionStage<Void>> actualAck) {
        return new Message<>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return metadata;
            }

            @Override
            public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
                return actualAck;
            }

        };
    }

    private static <T> Message<T> newMessage(T payload,
            Function<Metadata, CompletionStage<Void>> actualAck,
            BiFunction<Throwable, Metadata, CompletionStage<Void>> actualNack) {
        return new Message<>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
                return actualAck;
            }

            @Override
            public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
                return actualNack;
            }
        };
    }

    private static <T> Message<T> newMessage(T payload,
            Metadata metadata,
            Function<Metadata, CompletionStage<Void>> actualAck,
            BiFunction<Throwable, Metadata, CompletionStage<Void>> actualNack) {
        return new Message<>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return metadata;
            }

            @Override
            public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
                return actualAck;
            }

            @Override
            public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
                return actualNack;
            }
        };
    }

    /**
     * Create a message with the given payload.
     * No metadata are associated with the message, the acknowledgement and negative acknowledgement are immediate.
     *
     * @param payload The payload.
     * @param <T> The type of payload
     * @return A message with the given payload, no metadata, and no-op ack and nack functions.
     */
    static <T> Message<T> of(T payload) {
        return () -> payload;
    }

    /**
     * Create a message with the given payload and metadata.
     * The acknowledgement and negative-acknowledgement are immediate.
     *
     * @param payload The payload.
     * @param metadata The metadata, if {@code null} an empty set of metadata is used.
     * @param <T> The type of payload
     * @return A message with the given payload, metadata and no-op ack and nack functions.
     */
    static <T> Message<T> of(T payload, Metadata metadata) {
        return newMessage(payload, metadata == null ? Metadata.empty() : metadata);
    }

    /**
     * Create a message with the given payload and metadata.
     * The acknowledgement and negative-acknowledgement are immediate.
     *
     * @param payload The payload.
     * @param metadata The metadata, must not be {@code null}, must not contain {@code null} values, can be empty
     * @param <T> The type of payload
     * @return A message with the given payload, metadata and no-op ack and nack functions.
     */
    static <T> Message<T> of(T payload, Iterable<Object> metadata) {
        return newMessage(payload, Metadata.from(metadata));
    }

    /**
     * Create a message with the given payload and ack function.
     * No metadata are associated with the message.
     * Negative-acknowledgement is immediate.
     *
     * @param payload The payload.
     * @param ack The ack function, this will be invoked when the returned messages {@link #ack()} method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload, no metadata and ack function.
     */
    static <T> Message<T> of(T payload, Supplier<CompletionStage<Void>> ack) {
        return newMessage(payload, validateAck(ack));
    }

    /**
     * Create a message with the given payload and ack function.
     * No metadata are associated with the message.
     * Negative-acknowledgement is immediate.
     *
     * @param payload The payload.
     * @param ackM The ack function, this will be invoked when the returned messages {@link #ack(Metadata)} method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload, no metadata and ack function.
     */
    static <T> Message<T> of(T payload, Function<Metadata, CompletionStage<Void>> ackM) {
        return newMessage(payload, validateAck(ackM));
    }

    /**
     * Create a message with the given payload, metadata and ack function.
     * Negative-acknowledgement is immediate.
     *
     * @param payload The payload.
     * @param metadata the metadata, if {@code null}, empty metadata are used.
     * @param ack The ack function, this will be invoked when the returned messages {@link #ack()} method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload and ack function.
     */
    static <T> Message<T> of(T payload, Metadata metadata,
            Supplier<CompletionStage<Void>> ack) {
        return newMessage(payload, metadata == null ? Metadata.empty() : metadata, validateAck(ack));
    }

    /**
     * Create a message with the given payload, metadata and ack function.
     * Negative-acknowledgement is immediate.
     *
     * @param payload The payload.
     * @param metadata the metadata, must not be {@code null}, must not contain {@code null} values.
     * @param ack The ack function, this will be invoked when the returned messages {@link #ack()} method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload and ack function.
     */
    static <T> Message<T> of(T payload, Iterable<Object> metadata, Supplier<CompletionStage<Void>> ack) {
        return newMessage(payload, Metadata.from(metadata), validateAck(ack));
    }

    /**
     * Create a message with the given payload, metadata and ack function.
     * Negative-acknowledgement is immediate.
     *
     * @param payload The payload.
     * @param metadata the metadata, must not be {@code null}, must not contain {@code null} values.
     * @param ackM The ack function, this will be invoked when the returned messages {@link #ack(Metadata)} method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload and ack function.
     */
    static <T> Message<T> of(T payload, Iterable<Object> metadata, Function<Metadata, CompletionStage<Void>> ackM) {
        return newMessage(payload, Metadata.from(metadata), validateAck(ackM));
    }

    /**
     * Create a message with the given payload, ack and nack functions.
     *
     * @param payload The payload.
     * @param ack The ack function, this will be invoked when the returned messages {@link #ack()} method is invoked.
     * @param nack The negative-ack function, this will be invoked when the returned messages {@link #nack(Throwable)}
     *        method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload, metadata, ack and nack functions.
     */
    @Experimental("nack support is a SmallRye-only feature")
    static <T> Message<T> of(T payload, Supplier<CompletionStage<Void>> ack,
            Function<Throwable, CompletionStage<Void>> nack) {
        return newMessage(payload, validateAck(ack), validateNack(nack));
    }

    @Experimental("nack support is a SmallRye-only feature")
    static <T> Message<T> of(T payload, Function<Metadata, CompletionStage<Void>> ack,
            BiFunction<Throwable, Metadata, CompletionStage<Void>> nack) {
        return newMessage(payload, validateAck(ack), validateNack(nack));
    }

    /**
     * Create a message with the given payload, metadata and ack and nack functions.
     *
     * @param payload The payload.
     * @param metadata the metadata, must not be {@code null}, must not contain {@code null} values.
     * @param ack The ack function, this will be invoked when the returned messages {@link #ack()} method is invoked.
     * @param nack The negative-ack function, this will be invoked when the returned messages {@link #nack(Throwable)}
     *        method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload, metadata, ack and nack functions.
     */
    @Experimental("nack support is a SmallRye-only feature")
    static <T> Message<T> of(T payload, Iterable<Object> metadata,
            Supplier<CompletionStage<Void>> ack, Function<Throwable, CompletionStage<Void>> nack) {
        return newMessage(payload, Metadata.from(metadata), validateAck(ack), validateNack(nack));
    }

    /**
     * Create a message with the given payload, metadata and ack and nack functions.
     *
     * @param payload The payload.
     * @param metadata the metadata, must not be {@code null}, must not contain {@code null} values.
     * @param ackM The ack function, this will be invoked when the returned messages {@link #ack(Metadata)} method is invoked.
     * @param nackM The negative-ack function, this will be invoked when the returned messages
     *        {@link #nack(Throwable, Metadata)}
     *        method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload, metadata, ack and nack functions.
     */
    @Experimental("nack support is a SmallRye-only feature")
    static <T> Message<T> of(T payload, Iterable<Object> metadata, Function<Metadata, CompletionStage<Void>> ackM,
            BiFunction<Throwable, Metadata, CompletionStage<Void>> nackM) {
        return newMessage(payload, Metadata.from(metadata), validateAck(ackM), validateNack(nackM));
    }

    /**
     * Create a message with the given payload, metadata and ack and nack functions.
     *
     * @param payload The payload.
     * @param metadata the metadata, must not be {@code null}, must not contain {@code null} values.
     * @param ack The ack function, this will be invoked when the returned messages {@link #ack()} method is invoked.
     * @param nack The negative-ack function, this will be invoked when the returned messages {@link #nack(Throwable)}
     *        method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload, metadata, ack and nack functions.
     */
    @Experimental("metadata propagation is a SmallRye-specific feature")
    static <T> Message<T> of(T payload, Metadata metadata,
            Supplier<CompletionStage<Void>> ack, Function<Throwable, CompletionStage<Void>> nack) {
        return newMessage(payload, metadata == null ? Metadata.empty() : metadata, validateAck(ack), validateNack(nack));
    }

    /**
     * Create a message with the given payload, metadata and ack and nack functions.
     *
     * @param payload The payload.
     * @param metadata the metadata, must not be {@code null}, must not contain {@code null} values.
     * @param ackM The ack function, this will be invoked when the returned messages {@link #ack()} method is invoked.
     * @param nackM The negative-ack function, this will be invoked when the returned messages {@link #nack(Throwable)}
     *        method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload, metadata, ack and nack functions.
     */
    @Experimental("metadata propagation is a SmallRye-specific feature")
    static <T> Message<T> of(T payload, Metadata metadata,
            Function<Metadata, CompletionStage<Void>> ackM, BiFunction<Throwable, Metadata, CompletionStage<Void>> nackM) {
        return newMessage(payload, metadata == null ? Metadata.empty() : metadata, validateAck(ackM), validateNack(nackM));
    }

    /**
     * Creates a new instance of {@link Message} with the specified payload.
     * The metadata and ack/nack functions are taken from the current {@link Message}.
     *
     * @param payload the new payload.
     * @param <P> the type of the new payload
     * @return the new instance of {@link Message}
     */
    default <P> Message<P> withPayload(P payload) {
        return Message.of(payload, Metadata.from(getMetadata()), wrapAck(this), wrapNack(this));
    }

    /**
     * Creates a new instance of {@link Message} with the specified metadata.
     * The payload and ack/nack functions are taken from the current {@link Message}.
     *
     * @param metadata the metadata, must not be {@code null}, must not contain {@code null}.
     * @return the new instance of {@link Message}
     */
    default Message<T> withMetadata(Iterable<Object> metadata) {
        return Message.of(getPayload(), Metadata.from(metadata), wrapAck(this), wrapNack(this));
    }

    /**
     * Creates a new instance of {@link Message} with the specified metadata.
     * The payload and ack/nack functions are taken from the current {@link Message}.
     *
     * @param metadata the metadata, must not be {@code null}.
     * @return the new instance of {@link Message}
     */
    default Message<T> withMetadata(Metadata metadata) {
        return Message.of(getPayload(), Metadata.from(metadata), wrapAck(this), wrapNack(this));
    }

    /**
     * Creates a new instance of {@link Message} with the given acknowledgement supplier.
     * The payload, metadata and nack function are taken from the current {@link Message}.
     *
     * @param supplier the acknowledgement supplier
     * @return the new instance of {@link Message}
     */
    default Message<T> withAck(Supplier<CompletionStage<Void>> supplier) {
        return Message.of(getPayload(), getMetadata(), validateAck(supplier), wrapNack(this));
    }

    /**
     * Creates a new instance of {@link Message} with the given acknowledgement supplier.
     * The payload, metadata and nack function are taken from the current {@link Message}.
     *
     * @param supplier the acknowledgement supplier
     * @return the new instance of {@link Message}
     */
    @Experimental("metadata propagation is a SmallRye-specific feature")
    default Message<T> withAckWithMetadata(Function<Metadata, CompletionStage<Void>> supplier) {
        return Message.of(getPayload(), getMetadata(), supplier, wrapNack(this));
    }

    /**
     * Creates a new instance of {@link Message} with the given negative-acknowledgement function.
     * The payload, metadata and acknowledgment are taken from the current {@link Message}.
     *
     * @param nack the negative-acknowledgement function
     * @return the new instance of {@link Message}
     */
    @Experimental("metadata propagation is a SmallRye-specific feature")
    default Message<T> withNack(Function<Throwable, CompletionStage<Void>> nack) {
        return Message.of(getPayload(), getMetadata(), wrapAck(this), validateNack(nack));
    }

    /**
     * Creates a new instance of {@link Message} with the given negative-acknowledgement function.
     * The payload, metadata and acknowledgment are taken from the current {@link Message}.
     *
     * @param nack the negative-acknowledgement function
     * @return the new instance of {@link Message}
     */
    @Experimental("metadata propagation is a SmallRye-specific feature")
    default Message<T> withNackWithMetadata(BiFunction<Throwable, Metadata, CompletionStage<Void>> nack) {
        return Message.of(getPayload(), getMetadata(), wrapAck(this), nack);
    }

    /**
     * @return The payload for this message.
     */
    T getPayload();

    /**
     * @return The set of metadata attached to this message, potentially empty.
     */
    default Metadata getMetadata() {
        return Metadata.empty();
    }

    /**
     * Retrieves the metadata associated with the given class.
     *
     * @param clazz the class of the metadata to retrieve, must not be {@code null}
     * @return an {@link Optional} containing the associated metadata, empty if none.
     */
    @SuppressWarnings("unchecked")
    default <M> Optional<M> getMetadata(Class<? extends M> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("`clazz` must not be `null`");
        }
        // TODO we really should do `getMetadata().get(clazz)`, because it's more intuitive, corresponds
        //  closer to the documentation, and performs a bit better, but it would be a breaking change,
        //  as `Message.getMetadata(Class)` returns any metadata item that is a _subtype_ of given class,
        //  while `Metadata.get(Class)` returns a metadata item that is _exactly_ of given class
        for (Object item : getMetadata()) {
            if (clazz.isInstance(item)) {
                return Optional.of((M) item);
            }
        }
        return Optional.empty();
    }

    /**
     * @return the supplier used to retrieve the acknowledgement {@link CompletionStage}.
     */
    default Supplier<CompletionStage<Void>> getAck() {
        return () -> requireNonNullElse(getAckWithMetadata(), EMPTY_ACK).apply(this.getMetadata());
    }

    /**
     * @return the supplier used to retrieve the acknowledgement {@link CompletionStage}.
     */
    @Experimental("metadata propagation is a SmallRye-specific feature")
    default Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
        return null;
    }

    /**
     * @return the function used to retrieve the negative-acknowledgement asynchronous function.
     */
    default Function<Throwable, CompletionStage<Void>> getNack() {
        return reason -> requireNonNullElse(getNackWithMetadata(), EMPTY_NACK).apply(reason, this.getMetadata());
    }

    /**
     * @return the function used to retrieve the negative-acknowledgement asynchronous function.
     */
    @Experimental("metadata propagation is a SmallRye-specific feature")
    default BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
        return null;
    }

    /**
     * Acknowledge this message.
     *
     * @return a completion stage completed when the message is acknowledged. If the acknowledgement fails, the
     *         completion stage propagates the failure.
     */
    default CompletionStage<Void> ack() {
        return ack(this.getMetadata());
    }

    /**
     * Acknowledge this message.
     *
     * @return a completion stage completed when the message is acknowledged. If the acknowledgement fails, the
     *         completion stage propagates the failure.
     */
    @Experimental("metadata propagation is a SmallRye-specific feature")
    default CompletionStage<Void> ack(Metadata metadata) {
        Function<Metadata, CompletionStage<Void>> ackM = getAckWithMetadata();
        if (ackM != null) {
            return ackM.apply(metadata);
        } else {
            Supplier<CompletionStage<Void>> ack = getAck();
            if (ack != null) {
                return ack.get();
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    /**
     * Acknowledge negatively this message.
     * <code>nack</code> is used to indicate that the processing of a message failed. The reason is passed as parameter.
     *
     * @param reason the reason of the nack, must not be {@code null}
     * @return a completion stage completed when the message is negative-acknowledgement has completed. If the
     *         negative acknowledgement fails, the completion stage propagates the failure.
     */
    default CompletionStage<Void> nack(Throwable reason) {
        return nack(reason, this.getMetadata());
    }

    /**
     * Acknowledge negatively this message.
     * <code>nack</code> is used to indicate that the processing of a message failed. The reason is passed as parameter.
     * Additional metadata may be provided that the connector can use when nacking the message. The interpretation
     * of metadata is connector-specific.
     *
     * @param reason the reason of the nack, must not be {@code null}
     * @param metadata additional nack metadata the connector may use, may be {@code null}
     * @return a completion stage completed when the message is negative-acknowledgement has completed. If the
     *         negative acknowledgement fails, the completion stage propagates the failure.
     */
    @Experimental("nack support is a SmallRye-only feature; metadata propagation is a SmallRye-specific feature")
    default CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        if (reason == null) {
            throw new IllegalArgumentException("The reason must not be `null`");
        }
        BiFunction<Throwable, Metadata, CompletionStage<Void>> nackM = getNackWithMetadata();
        if (nackM != null) {
            return nackM.apply(reason, metadata);
        } else {
            Function<Throwable, CompletionStage<Void>> nack = getNack();
            if (nack == null) {
                LOGGER.warning(
                        String.format("A message has been nacked, but no nack function has been provided. The reason was: %s",
                                reason.getMessage()));
                LOGGER.finer(String.format("The full failure is: %s", reason));
                return CompletableFuture.completedFuture(null);
            } else {
                return nack.apply(reason);
            }
        }
    }

    /**
     * Returns an object of the specified type to allow access to the connector-specific {@link Message} implementation,
     * and other classes. For example, a Kafka connector could implement this method to allow unwrapping to a specific
     * Kafka message implementation, or to {@code ConsumerRecord} and {@code ProducerRecord}. If the {@link Message}
     * implementation does not support the target class, an {@link IllegalArgumentException} should be raised.
     * <p>
     * The default implementation tries to <em>cast</em> the current {@link Message} instance to the target class.
     * When a connector provides its own {@link Message} implementation, it should override this method to support
     * specific types.
     *
     * @param unwrapType the class of the object to be returned, must not be {@code null}
     * @param <C> the target type
     * @return an instance of the specified class
     * @throws IllegalArgumentException if the current {@link Message} instance does not support the call
     */
    default <C> C unwrap(Class<C> unwrapType) {
        if (unwrapType == null) {
            throw new IllegalArgumentException("The target class must not be `null`");
        }
        try {
            return unwrapType.cast(this);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Cannot unwrap an instance of " + this.getClass().getName()
                    + " to " + unwrapType.getName(), e);
        }

    }

    /**
     * Creates a new instance of {@link Message} with the current metadata, plus the given one.
     * The payload and ack/nack functions are taken from the current {@link Message}.
     *
     * @param metadata the metadata, must not be {@code null}.
     * @return the new instance of {@link Message}
     */
    default Message<T> addMetadata(Object metadata) {
        return Message.of(getPayload(), getMetadata().with(metadata), wrapAck(this), wrapNack(this));
    }

    /**
     * Apply the given modifier function to the current message returning the result for further composition
     *
     * @param modifier the function to modify the current message
     * @return the modified message
     * @param <R> the payload type of the modified message
     */
    default <R> Message<R> thenApply(Function<Message<T>, Message<R>> modifier) {
        return modifier.apply(this);
    }
}
