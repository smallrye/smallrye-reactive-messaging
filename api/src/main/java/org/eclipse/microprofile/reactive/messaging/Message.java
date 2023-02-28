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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
        if (metadata == null) {
            metadata = Metadata.empty();
        }
        Metadata actual = metadata;
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return actual;
            }
        };
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
        Metadata validated = Metadata.from(metadata);
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return validated;
            }
        };
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
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return Metadata.empty();
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return ack;
            }
        };
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
        if (metadata == null) {
            metadata = Metadata.empty();
        }
        Metadata actual = metadata;
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return actual;
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return ack;
            }
        };
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
    static <T> Message<T> of(T payload, Iterable<Object> metadata,
            Supplier<CompletionStage<Void>> ack) {
        Metadata validated = Metadata.from(metadata);
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return validated;
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return ack;
            }
        };
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
    static <T> Message<T> of(T payload,
            Supplier<CompletionStage<Void>> ack, Function<Throwable, CompletionStage<Void>> nack) {
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return Metadata.empty();
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return ack;
            }

            @Override
            public Function<Throwable, CompletionStage<Void>> getNack() {
                return nack;
            }
        };
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
        Metadata validated = Metadata.from(metadata);
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return validated;
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return ack;
            }

            @Override
            public Function<Throwable, CompletionStage<Void>> getNack() {
                return nack;
            }
        };
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
    static <T> Message<T> of(T payload, Metadata metadata,
            Supplier<CompletionStage<Void>> ack, Function<Throwable, CompletionStage<Void>> nack) {
        if (metadata == null) {
            metadata = Metadata.empty();
        }
        Metadata actual = metadata;
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return actual;
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return ack;
            }

            @Override
            public Function<Throwable, CompletionStage<Void>> getNack() {
                return nack;
            }
        };
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
        return Message.of(payload, Metadata.from(getMetadata()), getAck(), getNack());
    }

    /**
     * Creates a new instance of {@link Message} with the specified metadata.
     * The payload and ack/nack functions are taken from the current {@link Message}.
     *
     * @param metadata the metadata, must not be {@code null}, must not contains {@code null}.
     * @return the new instance of {@link Message}
     */
    default Message<T> withMetadata(Iterable<Object> metadata) {
        return Message.of(getPayload(), Metadata.from(metadata), getAck(), getNack());
    }

    /**
     * Creates a new instance of {@link Message} with the specified metadata.
     * The payload and ack/nack functions are taken from the current {@link Message}.
     *
     * @param metadata the metadata, must not be {@code null}.
     * @return the new instance of {@link Message}
     */
    default Message<T> withMetadata(Metadata metadata) {
        return Message.of(getPayload(), Metadata.from(metadata), getAck(), getNack());
    }

    /**
     * Creates a new instance of {@link Message} with the given acknowledgement supplier.
     * The payload, metadata and nack function are taken from the current {@link Message}.
     *
     * @param supplier the acknowledgement supplier
     * @return the new instance of {@link Message}
     */
    default Message<T> withAck(Supplier<CompletionStage<Void>> supplier) {
        return Message.of(getPayload(), getMetadata(), supplier, getNack());
    }

    /**
     * Creates a new instance of {@link Message} with the given negative-acknowledgement function.
     * The payload, metadata and acknowledgment are taken from the current {@link Message}.
     *
     * @param nack the negative-acknowledgement function
     * @return the new instance of {@link Message}
     */
    @Experimental("nack support is a SmallRye-only feature")
    default Message<T> withNack(Function<Throwable, CompletionStage<Void>> nack) {
        return Message.of(getPayload(), getMetadata(), getAck(), nack);
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
        return () -> CompletableFuture.completedFuture(null);
    }

    /**
     * @return the function used to retrieve the negative-acknowledgement asynchronous function.
     */
    @Experimental("nack support is a SmallRye-only feature")
    default Function<Throwable, CompletionStage<Void>> getNack() {
        return reason -> CompletableFuture.completedFuture(null);
    }

    /**
     * Acknowledge this message.
     *
     * @return a completion stage completed when the message is acknowledged. If the acknowledgement fails, the
     *         completion stage propagates the failure.
     */
    default CompletionStage<Void> ack() {
        Supplier<CompletionStage<Void>> ack = getAck();
        if (ack == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return ack.get();
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
    @Experimental("nack support is a SmallRye-only feature")
    default CompletionStage<Void> nack(Throwable reason) {
        return nack(reason, null);
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
        return Message.of(getPayload(), getMetadata().with(metadata), getAck(), getNack());
    }
}
