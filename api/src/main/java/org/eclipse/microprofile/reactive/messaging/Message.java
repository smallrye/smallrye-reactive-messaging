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

import org.eclipse.microprofile.reactive.messaging.spi.MessageBuilderProvider;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

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

    /**
     * @return The payload for this message.
     */
    T getPayload();

    /**
     * @return The set of metadata attached to this message, potentially empty.
     */
    Metadata getMetadata();

    /**
     * Retrieves the metadata associated with the given class.
     *
     * @param clazz the class of the metadata to retrieve, must not be {@code null}
     * @return an {@link Optional} containing the associated metadata, empty if none.
     */
    <M> Optional<M> getMetadata(Class<? extends M> clazz);

    /**
     * @return the supplier used to retrieve the acknowledgement {@link CompletionStage}.
     */
    Supplier<CompletionStage<Void>> getAck();

    /**
     * Acknowledge this message.
     *
     * @return a completion stage completed when the message is acknowledged. If the acknowledgement fails, the
     *         completion stage propagates the failure.
     */
    CompletionStage<Void> ack();

    static <T> MessageBuilder<T> newBuilder() {
        return MessageBuilderProvider.instance().newBuilder();
    }
}
