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

import java.util.concurrent.CompletionStage;

/**
 * Interface used to feed a stream from an <em>imperative</em> piece of code.
 * <p>
 * Instances are injected using:
 *
 * <pre>
 * &#64;Inject
 * &#64;Channel("my-channel")
 * Emitter&lt;String&gt; emitter;
 * </pre>
 * <p>
 * You can inject emitter sending payload or
 * {@link org.eclipse.microprofile.reactive.messaging.Message Messages}.
 * <p>
 * The name of the channel (given in the {@link Channel Channel annotation})
 * indicates which streams is fed. It must match the name used in a method using
 * {@link org.eclipse.microprofile.reactive.messaging.Incoming @Incoming} or an
 * outgoing stream configured in the application configuration.
 *
 * @param <T> type of payload or
 *        {@link org.eclipse.microprofile.reactive.messaging.Message
 *        Message}.
 */
public interface Emitter<T> {

    /**
     * Sends a payload to the stream.
     *
     * @param msg the <em>thing</em> to send, must not be {@code null}
     * @return the {@code CompletionStage}, which will be completed as sending the payload alone does not provide a callback
     *         mechnanism.
     * @throws IllegalStateException if the stream has been cancelled or terminated.
     */
    CompletionStage<Void> send(T msg);

    /**
     * Sends a message to the stream.
     * 
     * @param <M> the <em>Message</em> type
     * @param msg the <em>Message</em> to send, must not be {@code null}
     * @throws IllegalStateException if the stream has been cancelled or terminated.
     */
    <M extends Message<? extends T>> void send(M msg);

    /**
     * Completes the stream.
     * This method sends the completion signal, no messages can be sent once this method is called.
     */
    void complete();

    /**
     * Propagates an error in the stream.
     * This methods sends an error signal, no messages can be sent once this method is called.
     *
     * @param e the exception, must not be {@code null}
     */
    void error(Exception e);

    /**
     * @return {@code true} if the emitter has been terminated or the subscription cancelled.
     */
    boolean isCancelled();

    /**
     * @return {@code true} if the subscriber accepts messages, {@code false} otherwise.
     *         Using {@link #send(Object)} on an emitter not expecting message would throw an {@link IllegalStateException}.
     */
    boolean isRequested();

}
