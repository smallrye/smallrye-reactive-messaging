/**
 * Copyright (c) 2020 Contributors to the Eclipse Foundation
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

import java.util.concurrent.CompletionStage;

import io.smallrye.common.annotation.Experimental;

/**
 * Interface used to feed a channel from an <em>imperative</em> piece of code.
 * <p>
 * Instances are injected using:
 *
 * <pre>
 * &#64;Inject
 * &#64;Channel("my-channel")
 * Emitter&lt;String&gt; emitter;
 * </pre>
 * <p>
 * You can use an injected emitter to send either payloads or
 * {@link org.eclipse.microprofile.reactive.messaging.Message Messages}.
 * <p>
 * The name of the channel (given in the {@link Channel Channel annotation})
 * indicates which channel is fed. It must match the name used in a method using
 * {@link org.eclipse.microprofile.reactive.messaging.Incoming @Incoming} or an
 * outgoing channel configured in the application configuration.
 * <p>
 * The {@link OnOverflow OnOverflow annotation} can be used to configure what to do if
 * messages are sent using the `Emitter` when a downstream subscriber hasn't requested
 * more messages.
 *
 * @param <T> type of payload
 */
@Experimental("smallrye-only, added to the specification")
public interface Emitter<T> {

    /**
     * Sends a payload to the channel.
     * <p>
     * A {@link Message} object will be created to hold the payload and the returned {@code CompletionStage} will be completed
     * once this
     * {@code Message} is acknowledged. If the {@code Message} is never acknowledged, then the {@code CompletionStage} will
     * never be completed.
     *
     * @param msg the <em>thing</em> to send, must not be {@code null}
     * @return the {@code CompletionStage}, which will be completed when the message for this payload is acknowledged.
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     */
    CompletionStage<Void> send(T msg);

    /**
     * Sends a message to the channel.
     *
     * @param <M> the <em>Message</em> type
     * @param msg the <em>Message</em> to send, must not be {@code null}
     * @throws IllegalStateException if the channel has been cancelled or terminated or if an overflow strategy of
     *         {@link OnOverflow.Strategy#THROW_EXCEPTION THROW_EXCEPTION} or {@link OnOverflow.Strategy#BUFFER BUFFER} is
     *         configured and the emitter overflows.
     */
    <M extends Message<? extends T>> void send(M msg);

    /**
     * Sends the completion event to the channel indicating that no other events will be sent afterward.
     */
    void complete();

    /**
     * Sends a failure event to the channel. No more events will be sent afterward.
     *
     * @param e the exception, must not be {@code null}
     */
    void error(Exception e);

    /**
     * @return {@code true} if the emitter has been terminated or the subscription cancelled.
     */
    boolean isCancelled();

    /**
     * @return {@code true} if one or more subscribers request messages from the corresponding channel where the emitter
     *         connects to,
     *         return {@code false} otherwise.
     */
    boolean hasRequests();

}
