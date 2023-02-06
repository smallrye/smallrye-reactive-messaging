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
package io.smallrye.reactive.messaging.providers.wiring;

import jakarta.enterprise.util.AnnotationLiteral;

import io.smallrye.reactive.messaging.annotations.Broadcast;

/**
 * Supports inline instantiation of the {@link Broadcast} qualifier.
 */
public final class BroadcastLiteral extends AnnotationLiteral<Broadcast> implements Broadcast {

    private static final long serialVersionUID = 1L;

    private final int numberOfSubscribers;

    public static Broadcast of(int value) {
        return new BroadcastLiteral(value);
    }

    private BroadcastLiteral(int value) {
        this.numberOfSubscribers = value;
    }

    @Override
    public int value() {
        return numberOfSubscribers;
    }
}
