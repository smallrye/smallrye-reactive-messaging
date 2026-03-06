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
package io.smallrye.reactive.messaging.rabbitmq.og;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Provides methods to interact directly with a RabbitMQ instance using the original RabbitMQ Java client.
 */
public class RabbitMQUsage {

    private final static Logger LOGGER = Logger.getLogger(RabbitMQUsage.class);
    private final ConnectionFactory factory;
    private final String host;
    private final String username;
    private final String password;
    private final int managementPort;
    private Connection connection;
    private Channel channel;

    /**
     * Constructor.
     *
     * @param vertx the {@link io.vertx.mutiny.core.Vertx} instance (not used, kept for compatibility)
     * @param host the rabbitmq hostname
     * @param port the mapped rabbitmq port
     * @param managementPort the mapped rabbitmq management port
     * @param user user credential for accessing rabbitmq
     * @param pwd password credential for accessing rabbitmq
     */
    public RabbitMQUsage(final io.vertx.mutiny.core.Vertx vertx, final String host, final int port,
            final int managementPort, final String user, final String pwd) {
        this.host = host;
        this.username = user;
        this.password = pwd;
        this.managementPort = managementPort;

        this.factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(user);
        factory.setPassword(pwd);
        factory.setAutomaticRecoveryEnabled(false);
    }

    private void ensureConnected() throws IOException, java.util.concurrent.TimeoutException {
        if (connection == null || !connection.isOpen()) {
            connection = factory.newConnection();
        }
        if (channel == null || !channel.isOpen()) {
            channel = connection.createChannel();
        }
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
            AMQP.BasicProperties properties) {
        CountDownLatch done = new CountDownLatch(messageCount);

        Thread t = new Thread(() -> {
            LOGGER.debugf("Starting RabbitMQ sender to write %s messages with routing key %s", messageCount, routingKey);
            try {
                ensureConnected();

                for (int i = 0; i != messageCount; ++i) {
                    Object payload = messageSupplier.get();
                    byte[] body = payload.toString().getBytes(StandardCharsets.UTF_8);
                    channel.basicPublish(exchange, routingKey, properties, body);
                    LOGGER.debugf("Producer sent message %s", payload);
                    done.countDown();
                }
            } catch (Exception e) {
                LOGGER.error("Unable to send message", e);
            }
            LOGGER.debugf("Finished sending %s messages with routing key %s", messageCount, routingKey);
        });

        t.setName(exchange + "-producer-thread");
        t.start();

        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Use the supplied function to asynchronously consume messages from a queue.
     *
     * @param exchange the exchange
     * @param routingKey the routing key
     * @param consumerFunction the function to consume the messages; may not be null
     */
    public void consume(String exchange, String routingKey, Consumer<RabbitMQMessage> consumerFunction) {
        final String queue = "tempConsumeMessages";
        try {
            ensureConnected();
            channel.exchangeDeclare(exchange, "topic", false, true, null);
            channel.queueDeclare(queue, false, false, true, null);
            channel.queueBind(queue, exchange, routingKey);

            channel.basicConsume(queue, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                        AMQP.BasicProperties properties, byte[] body) throws IOException {
                    LOGGER.debugf("Consumer %s: consuming message", exchange);
                    RabbitMQMessage msg = new RabbitMQMessage(envelope, properties, body);
                    consumerFunction.accept(msg);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up consumer", e);
        }
    }

    /**
     * Use the supplied function to asynchronously consume messages from a queue.
     *
     * @param exchange the exchange
     * @param routingKey the routing key
     */
    public void prepareNackQueue(String exchange, String routingKey) {
        final String queue = "tempConsumeMessagesNack";
        try {
            ensureConnected();
            channel.exchangeDeclare(exchange, "topic", false, true, null);

            Map<String, Object> config = new HashMap<>();
            config.put("x-max-length", 1);
            config.put("x-overflow", "reject-publish");

            channel.queueDeclare(queue, false, false, true, config);
            channel.queueBind(queue, exchange, routingKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare nack queue", e);
        }
    }

    public AMQP.Queue.DeclareOk queueDeclareAndAwait(String queue, boolean durable, boolean exclusive,
            boolean autoDelete, JsonObject config) {
        try {
            ensureConnected();
            Map<String, Object> arguments = config != null ? config.getMap() : null;
            return channel.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
        } catch (Exception e) {
            throw new RuntimeException("Failed to declare queue", e);
        }
    }

    public void consumeIntegers(String exchange, String routingKey, Consumer<Integer> consumer) {
        final String queue = "tempConsumeIntegers";
        try {
            ensureConnected();
            LOGGER.debugf("RabbitMQ client now started");
            channel.exchangeDeclare(exchange, "topic", false, true, null);
            LOGGER.debugf("RabbitMQ exchange declared %s", exchange);
            channel.queueDeclare(queue, false, false, true, null);
            LOGGER.debugf("RabbitMQ queue declared %s", queue);
            LOGGER.debugf("About to bind RabbitMQ queue %s to exchange %s via routing key %s", queue, exchange, routingKey);
            channel.queueBind(queue, exchange, routingKey);
            LOGGER.debugf("RabbitMQ queue %s bound to exchange %s via routing key %s", queue, exchange, routingKey);

            channel.basicConsume(queue, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                        AMQP.BasicProperties properties, byte[] body) throws IOException {
                    final String payload = new String(body, StandardCharsets.UTF_8);
                    LOGGER.debugf("Consumer %s: consuming message %s", exchange, payload);
                    consumer.accept(Integer.parseInt(payload));
                }
            });
            LOGGER.debugf("Created consumer");
        } catch (Exception e) {
            throw new RuntimeException("Failed to consume integers", e);
        }
    }

    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            // Ignore
        }
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
     * named exchange and queue.
     *
     * @param exchangeName the name of the exchange
     * @param queueName the name of the queue
     * @return a {@link JsonArray} of binding descriptions
     * @throws IOException if an error occurs
     */
    public JsonArray getBindings(final String exchangeName, final String queueName) throws IOException {
        final URL url = new URL(String.format("http://%s:%d/api/bindings/%%2F/e/%s/q/%s",
                host, managementPort, exchangeName, queueName));
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
        final URL url = new URL(String.format("http://%s:%d/api/%s/%%2F/%s", host, managementPort,
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
        final String loginPassword = username + ":" + password;
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

    /**
     * Simple wrapper for RabbitMQ message
     */
    public static class RabbitMQMessage {
        private final Envelope envelope;
        private final AMQP.BasicProperties properties;
        private final byte[] body;

        public RabbitMQMessage(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            this.envelope = envelope;
            this.properties = properties;
            this.body = body;
        }

        public String bodyAsString() {
            return new String(body, StandardCharsets.UTF_8);
        }

        public byte[] body() {
            return body;
        }

        public Envelope envelope() {
            return envelope;
        }

        public AMQP.BasicProperties properties() {
            return properties;
        }
    }
}
