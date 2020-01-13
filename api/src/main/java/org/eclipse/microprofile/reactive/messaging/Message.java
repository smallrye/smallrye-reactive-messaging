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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * A message envelope.
 * <p>
 * Messaging providers may provide their own sub classes of this type, in order to allow messaging provider specific
 * information to be passed to and from applications.
 * </p>
 *
 * @param <T> The type of the message payload.
 */
public interface Message<T> {

    /**
     * Create a message with the given payload.
     * No metadata are associated with the message, the acknowledgement is immediate.
     *
     * @param payload The payload.
     * @param <T> The type of payload
     * @return A message with the given payload, no metadata, and a no-op ack function.
     */
    static <T> Message<T> of(T payload) {
        return () -> payload;
    }

    /**
     * Create a message with the given payload and metadata.
     * The acknowledgement is immediate.
     *
     * @param payload The payload.
     * @param metadata The metadata.
     * @param <T> The type of payload
     * @return A message with the given payload, metadata and a no-op ack function.
     */
    static <T> Message<T> of(T payload, Metadata metadata) {
        return new Message<T>() {
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

    /**
     * Create a message with the given payload and ack function.
     * No metadata are associated with the message.
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
     *
     * @param payload The payload.
     * @param metadata the metadata, if {@code null}, empty metadata are used.
     * @param ack The ack function, this will be invoked when the returned messages {@link #ack()} method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload and ack function.
     */
    static <T> Message<T> of(T payload, Metadata metadata, Supplier<CompletionStage<Void>> ack) {
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public Metadata getMetadata() {
                return metadata == null ? Metadata.empty() : metadata;
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return ack;
            }
        };
    }

    /**
     * Creates a new instance of {@link Message} with the specified payload.
     * The metadata and acknowledgment function are taken from the current {@link Message}.
     *
     * @param payload the new payload.
     * @param <P> the type of the new payload
     * @return the new instance of {@link Message}
     */
    default <P> Message<P> withPayload(P payload) {
        return Message.of(payload, getMetadata(), getAck());
    }

    /**
     * Creates a new instance of {@link Message} with the specified metadata.
     * The payload and acknowledgment function are taken from the current {@link Message}.
     *
     * @param metadata the metadata.
     * @return the new instance of {@link Message}
     */
    default Message<T> withMetadata(Metadata metadata) {
        return Message.of(getPayload(), metadata, getAck());
    }

    /**
     * Creates a new instance of {@link Message} with the given acknowledgement supplier.
     * The payload and metadata are taken from the current {@link Message}.
     *
     * @param supplier the acknowledgement supplier
     * @return the new instance of {@link Message}
     */
    default Message<T> withAck(Supplier<CompletionStage<Void>> supplier) {
        return Message.of(getPayload(), getMetadata(), supplier);
    }

    /**
     * @return The payload for this message.
     */
    T getPayload();

    /**
     * @return The set of metadata attached to this message.
     */
    default Metadata getMetadata() {
        return Metadata.empty();
    }

    /**
     * @return the supplier used to retrieve the acknowledgement {@link CompletionStage}.
     */
    default Supplier<CompletionStage<Void>> getAck() {
        return () -> CompletableFuture.completedFuture(null);
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
}
