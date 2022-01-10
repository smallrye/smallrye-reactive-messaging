package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

class HeadersPropagationTest extends KafkaTestBase {

    public static final String FIRST_HEADER_TO_KEEP_KEY = "HEADER_KEY_1";

    public static final String FIRST_HEADER_TO_KEEP_VALUE = "HEADER_VALUE_1";

    public static final String SECOND_HEADER_TO_KEEP_KEY = "HEADER_KEY_2";

    public static final String SECOND_HEADER_TO_KEEP_VALUE = "HEADER_VALUE_2";

    public static final String THIRD_HEADER_TO_FILTER_KEY = "HEADER_KEY_3";

    public static final String THIRD_HEADER_TO_FILTER_VALUE = "HEADER_VALUE_3";

    @Test
    void testFromKafkaToAppToKafka() {
        final List<Map.Entry<String, Integer>> messages = new CopyOnWriteArrayList<>();
        final List<Headers> receivedContexts = new CopyOnWriteArrayList<>();
        usage.consumeIntegersWithHeaders("result-topic", 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)),
                receivedContexts::add);
        final MyApp bean = runApplication(getKafkaSinkConfigForMyAppProcessingData(),
                MyApp.class);

        final AtomicInteger count = new AtomicInteger();
        final Headers producedheaders = new RecordHeaders();
        producedheaders.add(new RecordHeader(FIRST_HEADER_TO_KEEP_KEY, FIRST_HEADER_TO_KEEP_VALUE.getBytes()));
        producedheaders.add(new RecordHeader(SECOND_HEADER_TO_KEEP_KEY, SECOND_HEADER_TO_KEEP_VALUE.getBytes()));
        producedheaders.add(new RecordHeader(THIRD_HEADER_TO_FILTER_KEY, THIRD_HEADER_TO_FILTER_VALUE.getBytes()));
        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("parent-topic", null, null, "a-key", count.getAndIncrement(),
                        producedheaders));

        await().atMost(Duration.ofMinutes(5)).until(() -> messages.size() >= 10);
        final List<Integer> values = new ArrayList<>();
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.getValue()).isNotNull();
            values.add(entry.getValue());
        });
        assertThat(values).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        assertThat(receivedContexts).hasSize(10).doesNotContainNull();
        final long headerCount = receivedContexts.stream()
                .filter(headers -> {
                    return containsHeaderWithValue(headers, FIRST_HEADER_TO_KEEP_KEY, FIRST_HEADER_TO_KEEP_VALUE)
                            && containsHeaderWithValue(headers, SECOND_HEADER_TO_KEEP_KEY, SECOND_HEADER_TO_KEEP_VALUE)
                            && !containsHeaderWithValue(headers, THIRD_HEADER_TO_FILTER_KEY, THIRD_HEADER_TO_FILTER_VALUE);
                })
                .count();
        assertEquals(10, headerCount);
    }

    /**
     * Checks that the headers provided contains the header with key and value provided
     */
    private boolean containsHeaderWithValue(Headers headers, String headerKey,
            String headerValue) {
        Iterator<Header> iterator = headers.headers(headerKey).iterator();
        while (iterator.hasNext()) {
            final Header headerFromList = iterator.next();
            if (headerKey.equals(headerFromList.key()) && headerValue.equals(new String(headerFromList.value()))) {
                return true;
            }
        }
        return false;
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppProcessingData() {
        return kafkaConfig("mp.messaging.outgoing.target")
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", "result-topic")
                .put("propagate-headers", FIRST_HEADER_TO_KEEP_KEY + "," + SECOND_HEADER_TO_KEEP_KEY)
                .withPrefix("mp.messaging.incoming.source")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("topic", "parent-topic")
                .put("auto.offset.reset", "earliest");
    }

    @ApplicationScoped
    public static class MyApp {

        @Incoming("source")
        @Outgoing("target")
        public Integer processMessage(Integer input) {
            return input + 1;
        }
    }
}
