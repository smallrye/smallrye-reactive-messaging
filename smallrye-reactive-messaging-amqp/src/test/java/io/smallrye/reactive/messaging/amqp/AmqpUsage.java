package io.smallrye.reactive.messaging.amqp;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.proton.*;
import io.vertx.reactivex.core.Context;
import io.vertx.reactivex.core.Vertx;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.vertx.proton.ProtonHelper.message;
import static org.awaitility.Awaitility.await;

class AmqpUsage {

  private static Logger LOGGER = LoggerFactory.getLogger(AmqpUsage.class);
  private final Vertx vertx;
  private ProtonClient client;
  private ProtonConnection connection;
  private AtomicBoolean open = new AtomicBoolean();
  private Context context;


  AmqpUsage(Vertx vertx, String host, int port) {
    this(vertx, host, port, "artemis", "simetraehcapa");
  }

  private AmqpUsage(Vertx vertx, String host, int port, String user, String pwd) {
    CountDownLatch latch = new CountDownLatch(1);
    this.vertx = vertx;
    context = this.vertx.getOrCreateContext();
    context.runOnContext(x -> {
      client = ProtonClient.create(vertx.getDelegate());
      client.connect(new ProtonClientOptions()
        .setReconnectInterval(10)
        .setReconnectAttempts(100), host, port, user, pwd, getConnectionHandler(latch, host, port, user, pwd));
    });
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private Handler<AsyncResult<ProtonConnection>> getConnectionHandler(CountDownLatch latch, String host, int port, String user, String pwd) {
    return conn -> {
      if (conn.succeeded()) {
        this.connection = conn.result();
        this.connection
          .openHandler(connection -> {
            if (connection.succeeded()) {
              LOGGER.info("Connection to the AMQP broker succeeded");
              open.set(true);
              latch.countDown();
            } else {
              open.set(false);
              LOGGER.error("Failed to establish a connection with the AMQP broker", connection.cause());
            }
          })
          .closeHandler(c -> {
            LOGGER.info("Closing " + c.succeeded());
            if (c.failed()) {
              LOGGER.info("Error receive during the closing", c.cause());
            }
            open.set(false);
            LOGGER.info("Trying to re-open the connection");
            client.connect(new ProtonClientOptions()
              .setReconnectInterval(10)
              .setReconnectAttempts(100), host, port, user, pwd, getConnectionHandler(latch, host, port, user, pwd));
          })
          .disconnectHandler(d -> {
            LOGGER.info("Disconnecting");
            open.set(false);
          })
          .open();
      }
    };
  }

  /**
   * Use the supplied function to asynchronously produce messages and write them to the broker.
   *
   * @param topic           the topic, must not be null
   * @param messageCount    the number of messages to produce; must be positive
   * @param messageSupplier the function to produce messages; may not be null
   */
  void produce(String topic, int messageCount, Supplier<Object> messageSupplier) {
    CountDownLatch ready = new CountDownLatch(1);
    ProtonSender sender = connection.createSender(topic)
      .openHandler(s -> ready.countDown())
      .open();
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
          Message message = message();
          if (payload instanceof Section) {
            message.setBody((Section) payload);
          } else {
            message.setBody(new AmqpValue(payload));
          }
          message.setDurable(true);
          message.setTtl(10000);
          message.setDeliveryCount(1);
          CountDownLatch latch = new CountDownLatch(1);
          sleep();
          sender.send(message, x ->
            latch.countDown()
          );
          latch.await();
          LOGGER.info("Producer sent message {}", payload);
        }
      } catch (Exception e) {
        LOGGER.error("Unable to send message", e);
      } finally {
        closeQuietly(sender);
      }
    });
    t.setName(topic + "-thread");
    t.start();
  }

  private void sleep() {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void closeQuietly(ProtonSender sender) {
    try {
      sender.close();
    } catch (Exception e) {
      // Ignore me.
    }
  }

  void produceTenIntegers(String topic, Supplier<Integer> messageSupplier) {
    this.produce(topic, 10, messageSupplier::get);
  }

  /**
   * Use the supplied function to asynchronously consume messages from the cluster.
   *
   * @param topic            the topic
   * @param continuation     the function that determines if the consumer should continue; may not be null
   * @param consumerFunction the function to consume the messages; may not be null
   */
  private <T> void consume(String topic, BooleanSupplier continuation,
                           Consumer<AmqpMessage<T>> consumerFunction) {
    CountDownLatch latch = new CountDownLatch(1);
    ProtonReceiver receiver = getReceiver(topic);
    Thread t = new Thread(() -> {
      try {
        receiver.handler((delivery, message) -> {
          LOGGER.info("Consumer {}: consuming message {}", topic, message.getBody());
          consumerFunction.accept(new AmqpMessage<>(delivery, message));
          if (!continuation.getAsBoolean()) {
            receiver.close();
          }
        })
          .openHandler(r -> {
            LOGGER.info("Starting consumer to read messages on {}", topic);
            if (r.succeeded() && r.result().isOpen()) {
              latch.countDown();
            }
          })
          .open();
      } catch (Exception e) {
        LOGGER.error("Unable to receive messages from {}", topic, e);
      }
    });
    t.setName(topic + "-thread");
    t.start();

    try {
      latch.await(1, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private ProtonReceiver getReceiver(String topic) {
    int i = 0;
    while (i < 10) {
      try {
        return connection.createReceiver(topic);
      } catch (Exception e) {
        LOGGER.warn("Unable to create a receiver: {}", e.getMessage());
        i = i + 1;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          // Ignore me.
        }
      }
    }
    throw new RuntimeException("Unable to create a receiver after 10 attempts");
  }

  private void consumeStrings(String topic, BooleanSupplier continuation, Consumer<String> consumerFunction) {
    this.consume(topic, continuation, value -> consumerFunction.accept(value.getPayload().toString()));
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

  void close() {
    context.runOnContext(x -> {
      if (connection != null && !connection.isDisconnected()) {
        connection.close();
        connection.disconnect();
      }
    });
    try {
      await().until(() -> connection == null || connection.isDisconnected());
    } catch (Exception e) {
      // Ignore...
    }
  }
}
