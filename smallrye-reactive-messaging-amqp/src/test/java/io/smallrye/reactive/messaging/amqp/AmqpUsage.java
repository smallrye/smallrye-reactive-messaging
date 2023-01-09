/*
 * Copyright (c) 2018-2019 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *        The Eclipse Public License is available at
 *        http://www.eclipse.org/legal/epl-v10.html
 *
 *        The Apache License v2.0 is available at
 *        http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.smallrye.reactive.messaging.amqp;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.jboss.logging.Logger;

import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.amqp.impl.AmqpMessageImpl;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpMessageBuilder;
import io.vertx.mutiny.core.Vertx;
import io.vertx.proton.ProtonHelper;

public class AmqpUsage {

    private final static Logger LOGGER = Logger.getLogger(AmqpUsage.class);
    private final AmqpClient client;

    public AmqpUsage(Vertx vertx, String host, int port, String user, String pwd) {
        this.client = AmqpClient.create(new io.vertx.mutiny.core.Vertx(vertx.getDelegate()),
                new AmqpClientOptions().setHost(host).setPort(port).setUsername(user).setPassword(pwd));
    }

    /**
     * Use the supplied function to asynchronously produce messages and write them to the host.
     *
     * @param topic the topic, must not be null
     * @param messageCount the number of messages to produce; must be positive
     * @param messageSupplier the function to produce messages; may not be null
     */
    public void produce(String topic, int messageCount, Supplier<Object> messageSupplier) {
        CountDownLatch done = new CountDownLatch(messageCount);
        client.createSender(topic).subscribe().with(sender -> {
            Thread t = new Thread(() -> {
                LOGGER.infof("Starting AMQP sender to write %s messages", messageCount);
                try {
                    for (int i = 0; i != messageCount; ++i) {
                        Object payload = messageSupplier.get();
                        AmqpMessage msg;
                        if (payload instanceof AmqpMessage) {
                            msg = (AmqpMessage) payload;
                        } else if (payload instanceof io.vertx.amqp.AmqpMessage) {
                            msg = new AmqpMessage((io.vertx.amqp.AmqpMessage) payload);
                        } else if (payload instanceof Section) {
                            Message m = ProtonHelper.message();
                            m.setBody((Section) payload);
                            msg = new AmqpMessage(new AmqpMessageImpl(m));
                        } else {
                            AmqpMessageBuilder builder = io.vertx.mutiny.amqp.AmqpMessage.create()
                                    .durable(false)
                                    .ttl(10000);
                            if (payload instanceof Integer) {
                                builder.withIntegerAsBody((Integer) payload);
                            } else {
                                builder.withBody(payload.toString());
                            }
                            msg = builder.build();
                        }

                        sender.sendWithAck(msg).subscribe().with(x -> {
                            LOGGER.infof("Producer sent message %s", payload);
                            done.countDown();
                        }, Throwable::printStackTrace);

                    }
                } catch (Exception e) {
                    LOGGER.error("Unable to send message", e);
                    e.printStackTrace();
                }
            });
            t.setName(topic + "-thread");
            t.start();
        }, Throwable::printStackTrace);

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Ignore me
        }
    }

    /**
     * Use the supplied function to asynchronously consume messages from the cluster.
     *
     * @param topic the topic
     * @param consumerFunction the function to consume the messages; may not be null
     */
    public void consume(String topic,
            Consumer<io.vertx.mutiny.amqp.AmqpMessage> consumerFunction) {
        client.createReceiver(topic, new AmqpReceiverOptions().setDurable(true))
                .map(r -> r.handler(msg -> {
                    LOGGER.infof("Consumer %s: consuming message", topic);
                    consumerFunction.accept(msg);
                }))
                .await().indefinitely();
    }

    public void consumeIntegers(String topic, Consumer<Integer> consumer) {
        AmqpConnection connection = client
                .connectAndAwait();
        connection.createReceiver(topic, new AmqpReceiverOptions())
                .map(r -> r.handler(msg -> {
                    LOGGER.infof("Consumer %s: consuming message %s", topic, msg.bodyAsInteger());
                    consumer.accept(msg.bodyAsInteger());
                }))
                .await().indefinitely();
    }

    public void close() {
        client.closeAndAwait();
    }

    public void produceTenIntegers(String topic, Supplier<Integer> messageSupplier) {
        this.produce(topic, 10, messageSupplier::get);
    }

    public void consumeStrings(String topic, Consumer<String> consumerFunction) {
        this.consume(topic, value -> consumerFunction.accept(value.bodyAsString()));
    }
}
