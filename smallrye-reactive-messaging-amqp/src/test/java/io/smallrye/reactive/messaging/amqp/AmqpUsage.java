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

import io.vertx.core.Context;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.reactivex.core.Vertx;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import static io.vertx.proton.ProtonHelper.message;

public class AmqpUsage {

  private static Logger LOGGER = LoggerFactory.getLogger(AmqpUsage.class);
  private final Context context;
  private ProtonClient client;
  private ProtonConnection connection;

  private List<ProtonSender> senders = new CopyOnWriteArrayList<>();
  private List<ProtonReceiver> receivers = new CopyOnWriteArrayList<>();


  public AmqpUsage(Vertx vertx, String host, int port) {
    this(vertx, host, port, "artemis", "simetraehcapa");
  }


  public AmqpUsage(Vertx vertx, String host, int port, String user, String pwd) {
    CountDownLatch latch = new CountDownLatch(1);
    this.context = vertx.getDelegate().getOrCreateContext();
    context.runOnContext(x -> {
      client = ProtonClient.create(vertx.getDelegate());
      client.connect(host, port, user, pwd, conn -> {
        if (conn.succeeded()) {
          LOGGER.info("Connection to the AMQP host succeeded");
          this.connection = conn.result();
          this.connection
            .openHandler(connection -> latch.countDown())
            .open();
        }
      });
    });
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Use the supplied function to asynchronously produce messages and write them to the host.
   *
   * @param topic              the topic, must not be null
   * @param messageCount       the number of messages to produce; must be positive
   * @param messageSupplier    the function to produce messages; may not be null
   */
  void produce(String topic, int messageCount, Supplier<Object> messageSupplier) {
    CountDownLatch ready = new CountDownLatch(1);
    AtomicReference<ProtonSender> reference = new AtomicReference<>();
    context.runOnContext(x -> {
      ProtonSender sender = connection.createSender(topic);
      reference.set(sender);
      senders.add(sender);
      sender
        .openHandler(s -> ready.countDown())
        .open();
    });

    try {
      ready.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Thread t = new Thread(() -> {
      LOGGER.info("Starting AMQP sender to write {} messages", messageCount);
      try {
        for (int i = 0; i != messageCount; ++i) {
          Object payload = messageSupplier.get();
          org.apache.qpid.proton.message.Message message = message();
          if (payload instanceof Section) {
            message.setBody((Section) payload);
          } else if (payload != null) {
            message.setBody(new AmqpValue(payload));
          } else {
            // Don't set a body.
          }
          message.setDurable(true);
          message.setTtl(10000);
          CountDownLatch latch = new CountDownLatch(1);
          context.runOnContext((y) ->
            reference.get().send(message, x ->
              latch.countDown()
            )
          );
          latch.await();
          LOGGER.info("Producer sent message {}", payload);
        }
      } catch (Exception e) {
        LOGGER.error("Unable to send message", e);
      } finally {
        context.runOnContext(x -> reference.get().close());
      }
    });
    t.setName(topic + "-thread");
    t.start();

    try {
      ready.await();
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while waiting for the ProtonSender to be opened", e);
    }
  }

  /**
   * Use the supplied function to asynchronously consume messages from the cluster.
   *
   * @param topic            the topic
   * @param continuation     the function that determines if the consumer should continue; may not be null
   * @param consumerFunction the function to consume the messages; may not be null
   */
  private void consume(String topic, BooleanSupplier continuation,
                       Consumer<AmqpMessage> consumerFunction) {
    CountDownLatch latch = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      try {
        context.runOnContext(x -> {
          ProtonReceiver receiver = connection.createReceiver(topic);
          receivers.add(receiver);

          receiver.handler((delivery, message) -> {
            LOGGER.info("Consumer {}: consuming message {}", topic, message.getBody());
            consumerFunction.accept(new AmqpMessage(delivery, message));
            if (!continuation.getAsBoolean()) {
              receiver.close();
            }
          })
            .openHandler(r -> {
              LOGGER.info("Starting consumer to read messages on {}", topic);
              latch.countDown();
            })
            .open();
        });
      } catch (Exception e) {
        LOGGER.error("Unable to receive messages from {}", topic, e);
      }
    });
    t.setName(topic + "-thread");
    t.start();
    try {
      latch.await();
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while waiting for the ProtonReceiver to be opened", e);
    }
  }

  public void consumeIntegers(String topicName, int count, long timeout, TimeUnit unit, Consumer<Integer> consumer) {
    AtomicLong readCounter = new AtomicLong();
    this.consumeStrings(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit), s -> {
      consumer.accept(Integer.valueOf(s));
      readCounter.incrementAndGet();
    });
  }

  public void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Consumer<String> consumer) {
    AtomicLong readCounter = new AtomicLong();
    this.consumeStrings(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit), s -> {
      consumer.accept(s);
      readCounter.incrementAndGet();
    });
  }


  private BooleanSupplier continueIfNotExpired(BooleanSupplier continuation,
                                               long timeout, TimeUnit unit) {
    return new BooleanSupplier() {
      long stopTime = 0L;

      public boolean getAsBoolean() {
        if (this.stopTime == 0L) {
          this.stopTime = System.currentTimeMillis() + unit.toMillis(timeout);
        }

        return continuation.getAsBoolean() && System.currentTimeMillis() <= this.stopTime;
      }
    };
  }

  public void close() throws InterruptedException {
    CountDownLatch entities = new CountDownLatch(senders.size() + receivers.size());
    context.runOnContext(ignored -> {
      senders.forEach(sender -> {
        if (sender.isOpen()) {
          sender.closeHandler(x -> entities.countDown()).close();
        } else {
          entities.countDown();
        }
      });
      receivers.forEach(receiver -> {
        if (receiver.isOpen()) {
          receiver.closeHandler(x -> entities.countDown()).close();
        } else {
          entities.countDown();
        }
      });
    });

    entities.await(30, TimeUnit.SECONDS);

    if (connection != null && !connection.isDisconnected()) {
      CountDownLatch latch = new CountDownLatch(1);
      context.runOnContext(n ->
        connection
          .closeHandler(x -> latch.countDown())
          .close());
      latch.await(10, TimeUnit.SECONDS);
    }


  }

  void produceTenIntegers(String topic, Supplier<Integer> messageSupplier) {
    this.produce(topic, 10, messageSupplier::get);
  }

  private void consumeStrings(String topic, BooleanSupplier continuation, Consumer<String> consumerFunction) {
    this.consume(topic, continuation, value -> {
      consumerFunction.accept(value.getPayload().toString());
    });
  }

  private void consumeIntegers(String topic, BooleanSupplier continuation, Consumer<Integer> consumerFunction) {
    this.consume(topic, continuation, value -> consumerFunction.accept((Integer) value.getPayload()));
  }

  void consumeTenStrings(String topicName, Consumer<String> consumer) {
    AtomicLong readCounter = new AtomicLong();
    this.consumeStrings(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) 10), s -> {
      consumer.accept(s);
      readCounter.incrementAndGet();
    });
  }

  <T> void consumeTenMessages(String topicName, Consumer<AmqpMessage<T>> consumer) {
    AtomicLong readCounter = new AtomicLong();
    this.<T>consume(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) 10), s -> {
      consumer.accept(s);
      readCounter.incrementAndGet();
    });
  }

  void consumeTenIntegers(String topicName, Consumer<Integer> consumer) {
    AtomicLong readCounter = new AtomicLong();
    this.consumeIntegers(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) 10), s -> {
      consumer.accept(s);
      readCounter.incrementAndGet();
    });
  }

  private BooleanSupplier continueIfNotExpired(BooleanSupplier continuation) {
    return new BooleanSupplier() {
      long stopTime = 0L;

      public boolean getAsBoolean() {
        if (this.stopTime == 0L) {
          this.stopTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis((long) 10);
        }

        return continuation.getAsBoolean() && System.currentTimeMillis() <= this.stopTime;
      }
    };
  }
}

