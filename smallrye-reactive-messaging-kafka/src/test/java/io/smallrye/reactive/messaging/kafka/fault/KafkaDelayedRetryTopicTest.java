package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.getNextTopic;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class KafkaDelayedRetryTopicTest {

    static Stream<Arguments> retryTopic() {
        return Stream.of(
                Arguments.of(List.of("topic1", "topic2", "topic3"), -1, 0, "topic1"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), -1, 1, "topic2"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), -1, 2, "topic3"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), -1, 3, "deadTopic"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 0, 0, "topic1"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 0, 1, "topic2"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 0, 2, "topic3"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 0, 3, "deadTopic"),
                Arguments.of(List.of("topic1"), -1, 0, "topic1"),
                Arguments.of(List.of("topic1"), -1, 1, "deadTopic"),
                Arguments.of(List.of("topic1"), 0, 0, "topic1"),
                Arguments.of(List.of("topic1"), 0, 1, "deadTopic"));
    }

    static Stream<Arguments> retryTopicMaxRetries() {
        return Stream.of(
                Arguments.of(List.of("topic1", "topic2", "topic3"), 1, 0, "topic1"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 1, 1, "deadTopic"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 1, 2, "deadTopic"),

                Arguments.of(List.of("topic1", "topic2", "topic3"), 2, 0, "topic1"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 2, 1, "topic2"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 2, 2, "deadTopic"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 2, 3, "deadTopic"),

                Arguments.of(List.of("topic1", "topic2", "topic3"), 3, 0, "topic1"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 3, 1, "topic2"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 3, 2, "topic3"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 3, 3, "deadTopic"),

                Arguments.of(List.of("topic1", "topic2", "topic3"), 4, 0, "topic1"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 4, 1, "topic2"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 4, 2, "topic3"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 4, 3, "topic3"),
                Arguments.of(List.of("topic1", "topic2", "topic3"), 4, 4, "deadTopic"),

                Arguments.of(List.of("topic1"), 3, 1, "topic1"),
                Arguments.of(List.of("topic1"), 3, 2, "topic1"),
                Arguments.of(List.of("topic1"), 3, 3, "deadTopic"),
                Arguments.of(List.of("topic1"), 3, 4, "deadTopic"));
    }

    @ParameterizedTest
    @MethodSource("retryTopicMaxRetries")
    void getRetryTopicMaxRetries(List<String> topics, int maxRetries, int retryCount, String expected) {
        assertThat(getNextTopic(topics, "deadTopic", maxRetries, retryCount)).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("retryTopic")
    void getRetryTopic(List<String> topics, int maxRetries, int retryCount, String expected) {
        assertThat(getNextTopic(topics, "deadTopic", maxRetries, retryCount)).isEqualTo(expected);
    }
}
