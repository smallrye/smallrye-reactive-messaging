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
package io.smallrye.reactive.messaging.rabbitmq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.QueueOptions;
import io.vertx.rabbitmq.RabbitMQOptions;

/**
 * Provides methods to interact directly with a RabbitMQ instance.
 */
public class RabbitMQUsage {

    private final static Logger LOGGER = Logger.getLogger(RabbitMQUsage.class);
    private final RabbitMQClient client;
    private final RabbitMQOptions options;
    private final int managementPort;

    /**
     * Constructor.
     *
     * @param vertx the {@link Vertx} instance
     * @param host the rabbitmq hostname
     * @param port the mapped rabbitmq port
     * @param managementPort the mapped rabbitmq management port
     * @param user user credential for accessing rabbitmq
     * @param pwd password credential for accessing rabbitmq
     */
    public RabbitMQUsage(final Vertx vertx, final String host, final int port, final int managementPort, final String user,
            final String pwd) {
        this.managementPort = managementPort;
        this.options = new RabbitMQOptions().setHost(host).setPort(port).setUser(user).setPassword(pwd);
        this.client = RabbitMQClient.create(new Vertx(vertx.getDelegate()), options);
    }

    /**
     * Use the supplied function to asynchronously produce messages with default content_type and write them to the host.
     *
     * @param exchange the exchange, must not be null
     * @param messageCount the number of messages to produce; must be positive
     * @param messageSupplier the function to produce messages; may not be null
     */
    public void produce(String exchange, String queue, String routingKey, int messageCount, Supplier<Object> messageSupplier) {
        this.produce(exchange, queue, routingKey, messageCount, messageSupplier, "text/plain");
    }

    /**
     * Use the supplied function to asynchronously produce messages and write them to the host.
     *
     * @param exchange the exchange, must not be null
     * @param messageCount the number of messages to produce; must be positive
     * @param messageSupplier the function to produce messages; may not be null
     * @param contentType the message's content_type attribute
     */
    public void produce(String exchange, String queue, String routingKey, int messageCount, Supplier<Object> messageSupplier,
            String contentType) {
        this.produce(exchange, queue, routingKey, messageCount, messageSupplier,
                new AMQP.BasicProperties().builder().expiration("10000").contentType(contentType).build());
    }

    public void produce(String exchange, String queue, String routingKey, int messageCount, Supplier<Object> messageSupplier,
            BasicProperties properties) {
        CountDownLatch done = new CountDownLatch(messageCount);
        // Start the machinery to receive the messages
        client.startAndAwait();

        Thread t = new Thread(() -> {
            LOGGER.infof("Starting RabbitMQ sender to write %s messages with routing key %s", messageCount, routingKey);
            try {
                for (int i = 0; i != messageCount; ++i) {
                    Object payload = messageSupplier.get();
                    Buffer body = Buffer.buffer(payload.toString());
                    client.basicPublish(exchange, routingKey, properties, body)
                            .subscribe().with(
                                    v -> {
                                        LOGGER.infof("Producer sent message %s", payload);
                                        done.countDown();
                                    },
                                    Throwable::printStackTrace);
                }
            } catch (Exception e) {
                LOGGER.error("Unable to send message", e);
            }
            LOGGER.infof("Finished sending %s messages with routing key %s", messageCount, routingKey);
        });

        t.setName(exchange + "-thread");
        t.start();

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Ignore me
        }
    }

    /**
     * Use the supplied function to asynchronously consume messages from a queue.
     *
     * @param exchange the exchange
     * @param routingKey the routing key
     * @param consumerFunction the function to consume the messages; may not be null
     */
    public void consume(String exchange, String routingKey,
            Consumer<io.vertx.mutiny.rabbitmq.RabbitMQMessage> consumerFunction) {
        final String queue = "tempConsumeMessages";
        // Start by the machinery to receive the messages
        client.startAndAwait();
        client.exchangeDeclareAndAwait(exchange, "topic", false, true);
        client.queueDeclareAndAwait(queue, false, false, true);
        client.queueBindAndAwait(queue, exchange, routingKey);

        // Now set up a consumer
        client.basicConsumerAndAwait(queue, new QueueOptions()).handler(
                msg -> {
                    LOGGER.infof("Consumer %s: consuming message", exchange);
                    consumerFunction.accept(msg);
                });
    }

    public void consumeIntegers(String exchange, String routingKey, Consumer<Integer> consumer) {
        final String queue = "tempConsumeIntegers";
        // Start by the machinery to receive the messages
        client.startAndAwait();
        LOGGER.infof("RabbitMQ client now started");
        client.exchangeDeclareAndAwait(exchange, "topic", false, true);
        LOGGER.infof("RabbitMQ exchange declared %s", exchange);
        client.queueDeclareAndAwait(queue, false, false, true);
        LOGGER.infof("RabbitMQ queue declared %s", queue);
        LOGGER.infof("About to bind RabbitMQ queue % to exchange %s via routing key %s", queue, exchange, routingKey);
        client.queueBindAndAwait(queue, exchange, routingKey);
        LOGGER.infof("RabbitMQ queue % bound to exchange %s via routing key %s", queue, exchange, routingKey);

        // Now set up a consumer
        client.basicConsumerAndAwait(queue, new QueueOptions()).handler(
                msg -> {
                    final String payload = msg.body().toString();
                    LOGGER.infof("Consumer %s: consuming message %s", exchange, payload);
                    consumer.accept(Integer.parseInt(payload));
                });
        LOGGER.infof("Created consumer");
    }

    public void close() {
        client.stopAndAwait();
    }

    void produceTenIntegers(String exchange, String queue, String routingKey, Supplier<Integer> messageSupplier) {
        this.produce(exchange, queue, routingKey, 10, messageSupplier::get);
    }

    /**
     * Returns the RabbitMQ JSON representation of the named exchange.
     *
     * @param exchangeName the name of the exchange
     * @return a {@link JsonObject} describing the exchange
     * @throws IOException if an error occurs
     */
    public JsonObject getExchange(final String exchangeName) throws IOException {
        return getObjectByTypeAndName("exchanges", exchangeName);
    }

    /**
     * Returns the RabbitMQ JSON representation of the named queue.
     *
     * @param queueName the name of the queue
     * @return a {@link JsonObject} describing the queue
     * @throws IOException if an error occurs
     */
    public JsonObject getQueue(final String queueName) throws IOException {
        return getObjectByTypeAndName("queues", queueName);
    }

    /**
     * Returns the RabbitMQ JSON representation of the bindings between the
     * named exchange and queue..
     *
     * @param exchangeName the name of the exchange
     * @param queueName the name of the queue
     * @return a {@link JsonArray} of binding descriptions
     * @throws IOException if an error occurs
     */
    public JsonArray getBindings(final String exchangeName, final String queueName) throws IOException {
        final URL url = new URL(String.format("http://%s:%d/api/bindings/%%2F/e/%s/q/%s",
                options.getHost(), managementPort, exchangeName, queueName));
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Authorization", "Basic " + getBasicAuth());
        conn.connect();

        if (conn.getResponseCode() == 200) {
            final String jsonString = getResponseString(conn);
            return new JsonArray(jsonString);
        } else {
            return null;
        }
    }

    private JsonObject getObjectByTypeAndName(final String objectType, final String objectName) throws IOException {
        final URL url = new URL(String.format("http://%s:%d/api/%s/%%2F/%s", options.getHost(), managementPort,
                objectType, objectName));
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Authorization", "Basic " + getBasicAuth());
        conn.connect();
        if (conn.getResponseCode() == 200) {
            final String jsonString = getResponseString(conn);
            return new JsonObject(jsonString);
        } else {
            return null;
        }
    }

    private String getBasicAuth() {
        final String loginPassword = this.options.getUser() + ":" + this.options.getPassword();
        return Base64.getEncoder().encodeToString(loginPassword.getBytes());
    }

    private static String getResponseString(final HttpURLConnection conn) throws IOException {
        final BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
        final StringBuilder sb = new StringBuilder();
        String output;
        while ((output = br.readLine()) != null) {
            sb.append(output);
        }

        return sb.toString();
    }
}
