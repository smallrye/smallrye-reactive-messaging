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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Configure the acknowledgement policy for the given {@code @Incoming}.
 *
 * The set of supported acknowledgment policies depends on the method signature. The following list gives the supported
 * strategies for some common use cases.
 * Please refer to the specification for the full list.
 *
 * <ul>
 * <li><code> @Incoming("channel") void method(I payload)</code>: Post-processing (default), Pre-processing, None</li>
 * <li><code> @Incoming("channel") CompletionStage&lt;?&gt; method(I payload)</code>: Post-processing (default), Pre-processing,
 * None</li>
 * <li><code> @Incoming("in") @Outgoing("out") Message&lt;O&gt; method(Message&lt;I&gt; msg)</code>: Pre-processing,
 * None, Manual (default)</li>
 * <li><code> @Incoming("in") @Outgoing("out") O method(I payload)</code>: Post-Processing (default), Pre-processing, None</li>
 * </ul>
 *
 * Note that all messages must be acknowledged. An absence of acknowledgment is considered as a failure.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Acknowledgment {

    enum Strategy {

        /**
         * Acknowledgment managed by the user code. No automatic acknowledgment is performed. This strategy is only
         * supported by methods consuming {@link Message} instances.
         */
        MANUAL,

        /**
         * Acknowledgment performed automatically before the processing of the message by the user code.
         */
        PRE_PROCESSING,

        /**
         * Acknowledgment performed automatically once the message has been processed.
         * When {@code POST_PROCESSING} is used, the incoming message is acknowledged when the produced message is
         * acknowledged.
         *
         * Notice that this mode is not supported for all signatures. When supported, it's the default policy.
         *
         */
        POST_PROCESSING,

        /**
         * No acknowledgment is performed, neither implicitly or explicitly.
         * It means that the incoming messages are going to be acknowledged in a different location or using a different
         * mechanism.
         */
        NONE
    }

    /**
     * @return the acknowledgement policy.
     */
    Strategy value();

}
