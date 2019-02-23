package io.smallrye.reactive.messaging.kafka;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class KafkaSource<K, V> {
  private final PublisherBuilder<KafkaMessage> source;
  private final KafkaConsumer<K, V> consumer;

  KafkaSource(Vertx vertx, Config config) {
    ConfigurationHelper conf = ConfigurationHelper.create(config);
    Map<String, String> kafkaConfiguration = new HashMap<>();
    conf.asJsonObject().forEach(e -> kafkaConfiguration.put(e.getKey(), e.getValue().toString()));
    this.consumer = KafkaConsumer.create(vertx, kafkaConfiguration);
    String topic = config.getOptionalValue("topic", String.class)
      .orElseGet(() -> config.getValue("name", String.class));
    Objects.requireNonNull(topic, "The topic must be set, or the name must be set");
    Flowable<KafkaConsumerRecord<K, V>> flowable = consumer.toFlowable();

    if (config.getOptionalValue("retry", Boolean.class).orElse(true)) {
      Integer max = config.getOptionalValue("retry-attempts", Integer.class).orElse(5);
      flowable = flowable
        .retryWhen(attempts ->
          attempts
            .zipWith(Flowable.range(1, max), (n, i) -> i)
            .flatMap(i -> Flowable.timer(i, TimeUnit.SECONDS)));
    }

    if (config.getOptionalValue("broadcast", Boolean.class).orElse(false)) {
      flowable = flowable.publish().autoConnect();
    }

    this.source = ReactiveStreams.fromPublisher(
      flowable
        .doOnSubscribe(s ->
          // The Kafka subscription must happen on the subscription.
          this.consumer.subscribe(topic)
        ))
      .map(rec -> new ReceivedKafkaMessage<>(consumer, rec));
  }

  PublisherBuilder<KafkaMessage> getSource() {
    return source;
  }
}
