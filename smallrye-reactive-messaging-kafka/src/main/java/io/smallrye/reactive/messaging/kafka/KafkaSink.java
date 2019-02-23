package io.smallrye.reactive.messaging.kafka;

import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.reactivex.core.Vertx;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class KafkaSink {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
  private final KafkaWriteStream stream;
  private final int partition;
  private final String key;
  private final String topic;
  private final SubscriberBuilder<? extends Message, Void> subscriber;

  KafkaSink(Vertx vertx, Config config) {
    ConfigurationHelper helper = ConfigurationHelper.create(config);
    stream = KafkaWriteStream.create(vertx.getDelegate(), helper.asJsonObject().getMap());
    stream.exceptionHandler(t -> LOGGER.error("Unable to write to Kafka", t));

    partition = config.getOptionalValue("partition", Integer.class).orElse(0);
    key = config.getValue("key", String.class);
    topic = config.getValue("topic", String.class);
    if (topic == null) {
      LOGGER.warn("No default topic configured, only sending messages with an explicit topic set");
    }

    subscriber = ReactiveStreams.<Message>builder()
      .flatMapCompletionStage(message -> {
        ProducerRecord record;
        if (message instanceof KafkaMessage) {
          KafkaMessage km = ((KafkaMessage) message);

          if (this.topic == null && ((KafkaMessage) message).getTopic() == null) {
            LOGGER.error("Ignoring message - no topic set");
            return CompletableFuture.completedFuture(null);
          }

          record = new ProducerRecord<>(
            km.getTopic() == null ? this.topic : km.getTopic(),
            km.getPartition() == null ? this.partition : km.getPartition(),
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
          record
            = new ProducerRecord<>(topic, partition, null, key, message.getPayload());
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
      }).ignore();

  }

  SubscriberBuilder<? extends Message, Void> getSink() {
    return subscriber;
  }
}
