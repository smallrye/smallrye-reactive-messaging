package io.smallrye.reactive.messaging.eventbus;

import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class EventBusUsage {

  private static Logger LOGGER = LoggerFactory.getLogger(EventBusUsage.class);
  private final EventBus bus;

  public EventBusUsage(EventBus bus) {
    this.bus = bus;
  }

  /**
   * Use the supplied function to asynchronously produce messages and write them to the broker.
   *
   * @param topic              the topic, must not be null
   * @param messageCount       the number of messages to produce; must be positive
   * @param send               true to send, false to publish
   * @param completionCallback the function to be called when the producer is completed; may be null
   * @param messageSupplier    the function to produce messages; may not be null
   */
  public void produce(String topic, int messageCount, boolean send, Runnable completionCallback, Supplier<Object> messageSupplier) {
    Thread t = new Thread(() -> {
      LOGGER.info("Starting event bus client to write {} messages", messageCount);
      try {
        for (int i = 0; i != messageCount; ++i) {
          Object payload = messageSupplier.get();
          if (send) {
            bus.send(topic, payload);
          } else {
            bus.publish(topic, payload);
          }
          LOGGER.info("Producer sent message {}", payload);
        }
      } catch (Exception e) {
        LOGGER.error("Unable to send message", e);
      } finally {
        if (completionCallback != null) {
          completionCallback.run();
        }
      }
    });
    t.setName(topic + "-thread");
    t.start();
  }

  public void produceStrings(String topic, int messageCount, boolean send, Runnable completionCallback, Supplier<String> messageSupplier) {
    this.produce(topic, messageCount, send, completionCallback, messageSupplier::get);
  }

  public void produceIntegers(String topic, int messageCount, boolean send, Runnable completionCallback, Supplier<Integer> messageSupplier) {
    this.produce(topic, messageCount, send, completionCallback, messageSupplier::get);
  }

  /**
   * Use the supplied function to asynchronously consume messages from the cluster.
   *
   * @param topic            the topic
   * @param continuation     the function that determines if the consumer should continue; may not be null
   * @param consumerFunction the function to consume the messages; may not be null
   */
  public void consume(String topic, BooleanSupplier continuation,
                      Consumer<Object> consumerFunction) {
    CountDownLatch done = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      LOGGER.info("Starting consumer to read messages on {}", topic);
      try {
        MessageConsumer<Object> consumer = bus.consumer(topic);
        consumer
          .handler(msg -> {
            LOGGER.info("Consumer {}: consuming message {}", topic, msg.body());
            consumerFunction.accept(msg.body());
            if (!continuation.getAsBoolean()) {
              consumer.unregister();
            }
          })
          .completionHandler(x -> done.countDown());
      } catch (Exception e) {
        LOGGER.error("Unable to receive messages from {}", topic, e);
      }
    });
    t.setName(topic + "-thread");
    t.start();

    try {
      done.await(1, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void consumeStrings(String topic, BooleanSupplier continuation, Consumer<String> consumerFunction) {
    this.consume(topic, continuation, body -> consumerFunction.accept(body.toString()));
  }

  public void consumeIntegers(String topic, BooleanSupplier continuation, Consumer<Integer> consumerFunction) {
    this.consume(topic, continuation, body -> consumerFunction.accept((int) body));
  }

  public void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Consumer<String> consumer) {
    AtomicLong readCounter = new AtomicLong();
    this.consumeStrings(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit), s -> {
      consumer.accept(s);
      readCounter.incrementAndGet();
    });
  }

  public void consumeIntegers(String topicName, int count, long timeout, TimeUnit unit, Consumer<Integer> consumer) {
    AtomicLong readCounter = new AtomicLong();
    this.consumeIntegers(topicName, this.continueIfNotExpired(() -> readCounter.get() < (long) count, timeout, unit), s -> {
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
}
