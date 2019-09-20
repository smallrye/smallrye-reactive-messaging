package io.smallrye.reactive.messaging.kafka;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.propagation.TextMap;
import io.opentracing.util.GlobalTracer;
import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.reactivex.kafka.client.producer.KafkaHeader;

public class KafkaSource<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
    private final PublisherBuilder<? extends Message<?>> source;
    private final KafkaConsumer<K, V> consumer;

    KafkaSource(Vertx vertx, Config config, String servers) {
        Map<String, String> kafkaConfiguration = new HashMap<>();

        String group = config.getOptionalValue(ConsumerConfig.GROUP_ID_CONFIG, String.class).orElseGet(() -> {
            String s = UUID.randomUUID().toString();
            LOGGER.warn("No `group.id` set in the configuration, generate a random id: {}", s);
            return s;
        });

        JsonHelper.asJsonObject(config).forEach(e -> kafkaConfiguration.put(e.getKey(), e.getValue().toString()));
        kafkaConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, group);

        if (!kafkaConfiguration.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            LOGGER.info("Setting {} to {}", ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            kafkaConfiguration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!kafkaConfiguration.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            LOGGER.info("Key deserializer omitted, using String as default");
            kafkaConfiguration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }

        this.consumer = KafkaConsumer.create(vertx, kafkaConfiguration);
        String topic = getTopicOrFail(config);

        Objects.requireNonNull(topic, "The topic must be set, or the name must be set");

        Flowable<KafkaConsumerRecord<K, V>> flowable = consumer.toFlowable();

        if (config.getOptionalValue("retry", Boolean.class).orElse(true)) {
            Integer max = config.getOptionalValue("retry-attempts", Integer.class).orElse(5);
            flowable = flowable
                    .retryWhen(attempts -> attempts
                            .zipWith(Flowable.range(1, max), (n, i) -> i)
                            .flatMap(i -> Flowable.timer(i, TimeUnit.SECONDS)));
        }

        if (config.getOptionalValue("broadcast", Boolean.class).orElse(false)) {
            flowable = flowable.publish().autoConnect();
        }

        System.out.println("KafkaSource = consumer" + this.consumer);
        this.source = ReactiveStreams.fromPublisher(
                flowable
                        .doOnSubscribe(s -> {
                            // The Kafka subscription must happen on the subscription.
                            this.consumer.subscribe(topic);
                        }))
                .map(rec -> {
                    Tracer tracer = GlobalTracer.get();
                    SpanContext spanContext = tracer.extract(Builtin.TEXT_MAP, new TextMap() {
                        @Override
                        public Iterator<Entry<String, String>> iterator() {
                            Map<String, String> map = new LinkedHashMap<>();
                            Iterator<KafkaHeader> iterator = rec.headers().iterator();
                            while (iterator.hasNext()) {
                                KafkaHeader header = iterator.next();
                                map.put(header.key(), header.value().toString());
                            }
                            return map.entrySet().iterator();
                        }

                        @Override
                        public void put(String key, String value) {
                            throw new UnsupportedOperationException();
                        }
                    });
                    Span span = tracer.buildSpan("receive: " + rec.topic())
                            .addReference(References.FOLLOWS_FROM, spanContext)
                            .withTag("offset", rec.offset())
                            .withTag("partition", rec.partition())
                            .withTag("topic", rec.topic())
                            .withTag("key", String.valueOf(rec.key()))
                            .start();
                    span.finish();
                    System.out.println("KafkaSource/consumer " + rec.toString());
                    return new ReceivedKafkaMessage<>(consumer, rec, span);
                });
    }

    PublisherBuilder<? extends Message<?>> getSource() {
        return source;
    }

    void closeQuietly() {
        try {
            this.consumer.close(ar -> {
                if (ar.failed()) {
                    LOGGER.debug("An exception has been caught while closing the Kafka consumer", ar.cause());
                }
            });
        } catch (Throwable e) {
            LOGGER.debug("An exception has been caught while closing the Kafka consumer", e);
        }
    }

    private String getTopicOrFail(Config config) {
        return config.getOptionalValue("topic", String.class)
                .orElseGet(
                        () -> config.getOptionalValue("channel-name", String.class)
                                .orElseThrow(() -> new IllegalArgumentException("Topic attribute must be set")));
    }
}
