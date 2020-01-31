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

/**
 * Allows configuring the back pressure policy on injected {@link Emitter}:
 *
 * <pre>
 * {
 *     &#64;code
 *     &#64;Channel("channel")
 *     &#64;OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 256)
 *     Emitter&#60;String&#62; emitter;
 * }
 * </pre>
 *
 * <p>
 * When not used, a {@link OnOverflow.Strategy#BUFFER} strategy is used with a buffer limited to 128 elements.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ METHOD, CONSTRUCTOR, FIELD, PARAMETER })
public @interface OnOverflow {

    /**
     * The back pressure strategy.
     */
    enum Strategy {
        /**
         * Buffers <strong>all</strong> values until the downstream consumes it.
         * This creates a buffer with the size specified by {@link #bufferSize()} if present.
         * Otherwise, the size will be the value of the config property
         * <strong>mp.messaging.emitter.defult-buffer-size</strong>.
         * If the buffer is full, an error will be propagated.
         */
        BUFFER,
        /**
         * Buffers <strong>all</strong> values until the downstream consumes it.
         * This creates an unbound buffer. If the buffer is full, the application will die of {@code OutOfMemory}.
         */
        UNBOUNDED_BUFFER,
        /**
         * Drops the most recent value if the downstream can't keep up. It means that new value emitted by the upstream
         * are ignored.
         */
        DROP,

        /**
         * Propagates a failure in case the downstream can't keep up.
         */
        FAIL,

        /**
         * Keeps only the latest value, dropping any previous value if the downstream can't keep up.
         */
        LATEST,

        /**
         * The values are propagated without any back pressure strategy. It's the responsibility from the downstream to
         * implement a strategy to deal with overflow.
         */
        NONE
    }

    /**
     * @return the name of the strategy to be used on overflow.
     */
    Strategy value();

    /**
     * @return the size of the buffer when {@link Strategy#BUFFER} is used. If not set and if the {@link Strategy#BUFFER}
     *         strategy is used, the buffer size will be defaulted to the value of the config property
     *         mp.messaging.emitter.defult-buffer-size.
     */
    long bufferSize() default 0;

}
