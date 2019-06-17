/*******************************************************************************
 * Copyright (c) 2018 Contributors to the Eclipse Foundation
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
 ******************************************************************************/

package io.smallrye.reactive.messaging.tck;

import java.util.Objects;

import org.eclipse.microprofile.reactive.messaging.Message;

public class SimpleMessage<T> implements Message<T> {
    private final T payload;

    public SimpleMessage(T payload) {
        this.payload = payload;
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleMessage<?> that = (SimpleMessage<?>) o;
        return Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {

        return Objects.hash(payload);
    }

    @Override
    public String toString() {
        return "SimpleMessage{" +
                "payload=" + payload +
                '}';
    }

    public static <T> SimpleMessage<T> wrap(T payload) {
        return new SimpleMessage<>(payload);
    }
}
