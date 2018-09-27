package io.smallrye.reactive.messaging.kafka;

import io.reactivex.FlowableSubscriber;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.reactivex.core.Vertx;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.ReactiveStreams;
import org.reactivestreams.Subscriber;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.smallrye.reactive.messaging.kafka.ConfigHelper.*;

public class KafkaSink {
  private final KafkaWriteStream stream;
  private final int partition;
  private final Optional<Long> timestamp;
  private final String key;
  private final String topic;
  private final Subscriber<Message> subscriber;

  public KafkaSink(Vertx vertx, Map<String, String> config) {
    stream = KafkaWriteStream.create(vertx.getDelegate(), new HashMap<>(config));
    partition = getAsInteger(config, "partition", 0);
    timestamp = getAsLong(config, "timestamp");
    key = get(config, "key");
    topic = getOrDie(config, "topic");

    subscriber = ReactiveStreams.<Message>builder()
      .flatMapCompletionStage(message -> {
        ProducerRecord record
          = new ProducerRecord<>(topic, partition, timestamp.orElse(null), key, message.getPayload());
        CompletableFuture<Void> future = new CompletableFuture<>();

        Handler<AsyncResult> handler = ar -> {
          if (ar.succeeded()) {
            future.complete(null);
          } else {
            future.completeExceptionally(ar.cause());
          }
        };

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
