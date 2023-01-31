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

import static java.lang.annotation.ElementType.*;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.enterprise.util.Nonbinding;
import jakarta.inject.Qualifier;

import io.smallrye.common.annotation.Experimental;

/**
 * This qualifier indicates which channel should be injected / populated.
 * <p>
 * This qualifier can be used to inject a <em>Channel</em> containing the items and signals propagated by the specified
 * channel. For example, it can be used to {@code @Inject} a {@code Publisher} representing a channel managed by the
 * Reactive Messaging implementation.
 * <p>
 * Can be injected:
 * <ul>
 * <li>Publisher&lt;X&gt; with X the payload type</li>
 * <li>Publisher&lt;Message&lt;X&gt;&gt; with X the payload type</li>
 * <li>PublisherBuilder&lt;Message&lt;X&gt;&gt; with X the payload type</li>
 * <li>PublisherBuilder&lt;X&gt; with X the payload type</li>
 * </ul>
 * <p>
 * When this qualifier is used on an {@link Emitter}, it indicates which channel received the emitted values / signals:
 *
 * <pre>
 * <code>
 * &#64;Inject @Channel("my-channel") Emitter&lt;String&gt; emitter;
 *
 * // ...
 * emitter.send("a");
 * </code>
 * </pre>
 *
 * A subscriber for the above channel must be found by the time a message is emitted to the channel.
 * Otherwise, {@code IllegalStateException} must be thrown.
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ METHOD, CONSTRUCTOR, FIELD, PARAMETER })
@Experimental("smallrye-only, added to the specification")
public @interface Channel {

    /**
     * The name of the channel (indicated in the {@code @Outgoing} annotation.
     *
     * @return the channel name, mandatory, non-{@code null} and non-blank. It must matches one of the available channels.
     */
    @Nonbinding
    String value();
}
