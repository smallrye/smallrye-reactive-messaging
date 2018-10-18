package io.smallrye.reactive.messaging.kafka;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.spi.ConfigurationHelper;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class KafkaSource {
  private final Flowable<KafkaMessage> source;

  public <K, T> KafkaSource(Vertx vertx, Map<String, String> config) {
    ConfigurationHelper conf = ConfigurationHelper.create(config);
    KafkaConsumer<K, T> consumer = KafkaConsumer.<K, T>create(vertx, config).subscribe(conf.getOrDie("topic"));
    Flowable<KafkaConsumerRecord<K, T>> flowable = consumer.toFlowable();


    if (conf.getAsBoolean("retry", true)) {
      flowable = flowable
        .retryWhen(attempts ->
          attempts
            .zipWith(Flowable.range(1, conf.getAsInteger( "retry-attempts", 5)), (n, i) -> i)
            .flatMap(i -> Flowable.timer(i, TimeUnit.SECONDS)));
    }

    if (conf.getAsBoolean( "multicast", false)) {
      flowable = flowable.publish().autoConnect();
    }

    this.source = flowable
      .map(rec -> new ReceivedKafkaMessage<>(consumer, rec));
  }

  public static CompletionStage<Publisher<? extends Message>> create(Vertx vertx, Map<String, String> config) {
    return CompletableFuture.completedFuture(new KafkaSource(vertx, config).source);
  }

  public Flowable<KafkaMessage> getSource() {
    return source;
  }
}
