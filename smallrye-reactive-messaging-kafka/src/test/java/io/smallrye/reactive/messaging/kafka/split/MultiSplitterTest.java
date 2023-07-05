package io.smallrye.reactive.messaging.kafka.split;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.split.MultiSplitter;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;

class MultiSplitterTest extends KafkaCompanionTestBase {

    @Test
    public void sink() {
        String sinkTopic = topic + "-sink";
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.outgoing.sink")
                .with("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .with("topic", sinkTopic)
                .withPrefix("mp.messaging.outgoing.sink2")
                .with("topic", sinkTopic + 2)
                .with("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        addBeans(Source.class);
        runApplication(config, MySplitProcessorBean.class);

        ConsumerTask<String, String> records = companion.consumeStrings()
                .fromTopics(sinkTopic, 1500)
                .awaitCompletion();
        assertThat(records)
                .extracting(ConsumerRecord::value)
                .containsSequence(IntStream.range(1, 1500).map(i -> i * 2).mapToObj(String::valueOf)
                        .collect(Collectors.toList()));

        companion.consumeStrings()
                .fromTopics(sinkTopic + 2, 1500)
                .awaitCompletion();
    }

    @ApplicationScoped
    public static class MySplitProcessorBean {

        private final List<String> records = new CopyOnWriteArrayList<>();

        enum OddEven {
            EVEN,
            ODD
        }

        @Incoming("data")
        @Outgoing("sink")
        @Outgoing("sink2")
        public MultiSplitter<String, OddEven> process(Multi<String> recordMulti) {
            return recordMulti
                    .onItem().invoke(records::add)
                    .split(OddEven.class, s -> (Integer.parseInt(s) % 2 == 0) ? OddEven.EVEN : OddEven.ODD);
        }

        public List<String> list() {
            return records;
        }

    }

    @ApplicationScoped
    public static class Source {
        @Outgoing("data")
        Multi<String> produce() {
            return Multi.createFrom().range(1, 3001).map(String::valueOf);
        }
    }

}
