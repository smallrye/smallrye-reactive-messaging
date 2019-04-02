package io.smallrye.reactive.messaging.kafka;

import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.reactivex.core.Vertx;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

class KafkaSink {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
  private final KafkaWriteStream stream;
  private final int partition;
  private final String key;
  private final String topic;
  private final SubscriberBuilder<? extends Message, Void> subscriber;

  KafkaSink(Vertx vertx, Config config) {
    ConfigurationHelper helper = ConfigurationHelper.create(config);
    JsonObject entries = helper.asJsonObject();

    // Acks must be a string, even when "1".
    if (entries.containsKey("acks")) {
      entries.put("acks", entries.getValue("acks").toString());
    }
    stream = KafkaWriteStream.create(vertx.getDelegate(), entries.getMap());
    stream.exceptionHandler(t -> LOGGER.error("Unable to write to Kafka", t));

    partition = config.getOptionalValue("partition", Integer.class).orElse(-1);
    key = config.getOptionalValue("key", String.class).orElse(null);
    topic = config.getOptionalValue("topic", String.class).orElse(null);
    if (topic == null) {
      LOGGER.warn("No default topic configured, only sending messages with an explicit topic set");
    }

    subscriber = ReactiveStreams.<Message>builder()
      .flatMapCompletionStage(message -> {
        try {
          ProducerRecord record;
          if (message instanceof KafkaMessage) {
            KafkaMessage km = ((KafkaMessage) message);

            if (this.topic == null && km.getTopic() == null) {
              LOGGER.error("Ignoring message - no topic set");
              return CompletableFuture.completedFuture(null);
            }

            Integer actualPartition = null;
            if (this.partition != -1) {
              actualPartition = this.partition;
            }
            if (km.getPartition() != null) {
              actualPartition = km.getPartition();
            }

            record = new ProducerRecord<>(
              km.getTopic() == null ? this.topic : km.getTopic(),
              actualPartition,
              km.getTimestamp(),
              km.getKey() == null ? this.key : km.getKey(),
              km.getPayload(),
              km.getHeaders().unwrap()
            );
            LOGGER.info("Sending message {} to Kafka topic '{}'", message, record.topic());
          } else {
            if (this.topic == null) {
              LOGGER.error("Ignoring message - no topic set");
              return CompletableFuture.completedFuture(null);
            }
            if (partition == -1) {
              record = new ProducerRecord<>(topic, null, null, key, message.getPayload());
            } else {
              record
                = new ProducerRecord<>(topic, partition, null, key, message.getPayload());
            }
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
        } catch (RuntimeException e) {
          LOGGER.error("Unable to send a record to Kafka ", e);
          return CompletableFuture.completedFuture(message);
        }
      }).ignore();

  }

  SubscriberBuilder<? extends Message, Void> getSink() {
    return subscriber;
  }

  void close() {
    this.stream.close();
  }
}
