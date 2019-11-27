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

/**
 * The MicroProfile Reactive Messaging API Connector SPI
 * <p>
 * This package provides the SPI to implement {@code connectors}. A {@code connector} can be seen as a Reactive Messaging
 * plug-in to support a specific messaging technology. For example, you can have a Kafka connector to deal with Kafka,
 * an AMQP connector to interact with AMQP brokers and routers and so on. Connector implementation should be
 * agnostic to the Reactive Messaging implementation.
 *
 * A connector can be seen as:
 * <ul>
 * <li>a source of messages - it retrieves messages and injects them into the Reactive Messaging application. To
 * manage this direction, the connector implementation must implement the
 * {@link org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory} interface.</li>
 * <li>a sink of messages - it forwards messages emitted by the Reactive Messaging application to the managed
 * technology. To achieve this, the connector implementation must implement the
 * {@link org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory} interface.</li>
 * </ul>
 *
 * Connectors are implemented as CDI beans and identified using the
 * {@link org.eclipse.microprofile.reactive.messaging.spi.Connector} qualifier. Connectors receive the channel
 * configuration matching their {@link Connector} name.
 */
@org.osgi.annotation.versioning.Version("1.1")
package org.eclipse.microprofile.reactive.messaging.spi;
