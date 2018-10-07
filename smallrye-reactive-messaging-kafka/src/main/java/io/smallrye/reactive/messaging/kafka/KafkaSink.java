package io.smallrye.reactive.messaging.kafka;

import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.reactivex.core.Vertx;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Subscriber;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.smallrye.reactive.messaging.spi.ConfigurationHelper.*;

public class KafkaSink {
  private static final Logger LOGGER = LogManager.getLogger(KafkaSink.class);
  private final KafkaWriteStream stream;
  private final int partition;
  private final Optional<Long> timestamp;
  private final String key;
  private final String topic;
  private final Subscriber<Message> subscriber;

  public KafkaSink(Vertx vertx, Map<String, String> config) {
    stream = KafkaWriteStream.create(vertx.getDelegate(), new HashMap<>(config));
    ConfigurationHelper conf = ConfigurationHelper.create(config);
    partition = conf.getAsInteger("partition", 0);
    timestamp = conf.getAsLong( "timestamp");
    key = conf.get("key");
    topic = conf.get("topic");
    if (topic == null) {
      LOGGER.warn("No default topic configured, only sending messages with an explicit topic set");
    }

    subscriber = ReactiveStreams.<Message>builder()
      .flatMapCompletionStage(message -> {
        ProducerRecord record;
        if (message instanceof  KafkaMessage) {
          KafkaMessage km = ((KafkaMessage) message);

          if (this.topic == null  && ((KafkaMessage) message).getTopic() == null) {
            LOGGER.error("Ignoring message - no topic set");
            return CompletableFuture.completedFuture(null);
          }

          record = new ProducerRecord<>(
            km.getTopic() == null ? this.topic : km.getTopic(),
            km.getPartition() == null ? this.partition : km.getPartition(),
            km.getTimestamp() == null ? this.timestamp.orElse(null) : km.getTimestamp(),
            km.getKey() == null ? this.key  : km.getKey(),
            km.getPayload(),
            km.getHeaders().unwrap()
          );
          LOGGER.info("Sending message {} to Kafka topic '{}'", message, record.topic());
        } else {
          if (this.topic == null) {
            LOGGER.error("Ignoring message - no topic set");
            return CompletableFuture.completedFuture(null);
          }
          record
            = new ProducerRecord<>(topic, partition, timestamp.orElse(null), key, message.getPayload());
        }

        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        Handler<AsyncResult<RecordMetadata>> handler = ar -> {
          if (ar.succeeded()) {
            LOGGER.info("Message {} sent successfully to Kafka topic {}", message, record.topic());
            future.complete(ar.result());
          } else {
            LOGGER.error("Message {} was not sent to Kafka topic {}", message, record.topic(), ar.cause());
            future.completeExceptionally(ar.cause());
          }
        };

        LOGGER.debug("Using stream {} to write the record {}", stream, record);
        stream.write(record, handler);
        return future;
      }).ignore().build();

  }

  public static CompletionStage<Subscriber<? extends Message>> create(Vertx vertx, Map<String, String> config) {
    return CompletableFuture.completedFuture(new KafkaSink(vertx, config).subscriber);
  }

  public Subscriber<Message> getSubscriber() {
    return subscriber;
  }
}
