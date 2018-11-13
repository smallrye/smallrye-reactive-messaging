package io.smallrye.reactive.messaging.amqp;

import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.reactivex.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.vertx.proton.ProtonHelper.message;
import static org.awaitility.Awaitility.await;

class AmqpUsage {

  private static Logger LOGGER = LogManager.getLogger(AmqpUsage.class);
  private ProtonClient client;
  private ProtonConnection connection;


  AmqpUsage(Vertx vertx, String host, int port) {
    this(vertx, host, port, "artemis", "simetraehcapa");
  }

  private AmqpUsage(Vertx vertx, String host, int port, String user, String pwd) {
    CountDownLatch latch = new CountDownLatch(1);
    vertx.runOnContext(x -> {
      client = ProtonClient.create(vertx.getDelegate());
      client.connect(host, port, user, pwd, conn -> {
        if (conn.succeeded()) {
          LOGGER.info("Connection to the AMQP broker succeeded");
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
   * Use the supplied function to asynchronously produce messages and write them to the broker.
   *  @param topic              the topic, must not be null
   * @param messageCount       the number of messages to produce; must be positive
   * @param messageSupplier    the function to produce messages; may not be null
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
          sender.send(message, x ->
            latch.countDown()
          );
          latch.await();
          LOGGER.info("Producer sent message {}", payload);
        }
      } catch (Exception e) {
        LOGGER.error("Unable to send message", e);
      } finally {
        sender.close();
      }
    });
    t.setName(topic + "-thread");
    t.start();
  }

  void produceTenIntegers(String topic, Supplier<Integer> messageSupplier) {
    this.produce(topic, 10, messageSupplier::get);
  }

  /**
   * Use the supplied function to asynchronously consume messages from the cluster.
   *  @param topic            the topic
   * @param continuation     the function that determines if the consumer should continue; may not be null
   * @param consumerFunction the function to consume the messages; may not be null
   */
  private <T> void consume(String topic, BooleanSupplier continuation,
                           Consumer<AmqpMessage<T>> consumerFunction) {
    ProtonReceiver receiver = connection.createReceiver(topic);
    Thread t = new Thread(() -> {
      try {
        receiver.handler((delivery, message) -> {
          LOGGER.info("Consumer {}: consuming message {}", topic, message.getBody());
          consumerFunction.accept(new AmqpMessage<>(delivery, message));
          if (!continuation.getAsBoolean()) {
            receiver.close();
          }
        })
          .openHandler(r -> LOGGER.info("Starting consumer to read messages on {}", topic))
          .open();
      } catch (Exception e) {
        LOGGER.error("Unable to receive messages from {}", topic, e);
      }
    });
    t.setName(topic + "-thread");
    t.start();
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
    if (connection != null && !connection.isDisconnected()) {
      connection.close();
      connection.disconnect();
      await().until(() -> connection.isDisconnected());
    }
  }
}
