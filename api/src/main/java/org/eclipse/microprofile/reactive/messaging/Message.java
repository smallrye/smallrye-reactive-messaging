/**
 * Copyright (c) 2018-2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
     *
     * @param payload The payload.
     * @param <T> The type of payload
     * @return A message with the given payload, and a no-op ack function.
     */
    static <T> Message<T> of(T payload) {
        return () -> payload;
    }

    /**
     * Create a message with the given payload and ack function.
     *
     * @param payload The payload.
     * @param ack The ack function, this will be invoked when the returned messages {@link #ack()} method is invoked.
     * @param <T> the type of payload
     * @return A message with the given payload and ack function.
     */
    static <T> Message<T> of(T payload, Supplier<CompletionStage<Void>> ack) {
        return new Message<T>() {
            @Override
            public T getPayload() {
                return payload;
            }

            @Override
            public CompletionStage<Void> ack() {
                return ack.get();
            }
        };
    }

    /**
     * @return The payload for this message.
     */
    T getPayload();

    /**
     * Acknowledge this message.
     *
     * @return a completion stage completed when the message is acknowledged. If the acknowledgement fails, the
     *         completion stage propagates the failure.
     */
    default CompletionStage<Void> ack() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Returns an object of the specified type to allow access to the connector-specific {@link Message} implementation,
     * and other classes. For example, a Kafka connector could implement this method to allow unwrapping to a specific
     * Kafka message implementation, or to {@code ConsumerRecord} and {@code ProducerRecord}. If the {@link Message}
     * implementation does not support the target class, an {@link IllegalArgumentException} should be raised.
     *
     * The default implementation tries to <em>cast</em> the current {@link Message} instance to the target class.
     * When a connector provides its own {@link Message} implementation, it should override this method to support
     * specific types.
     *
     * @param unwrapType the class of the object to be returned, must not be {@code null}
     * @param <C> the target type
     * @return an instance of the specified class
     * @throws IllegalArgumentException if the current {@link Message} instance does not support the call
     */
    @SuppressWarnings({ "unchecked" })
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
