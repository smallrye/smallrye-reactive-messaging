package io.smallrye.reactive.messaging.kafka;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;

public class AssignSeekConfigTest {

    static Stream<Arguments> checkOffsets() {
        return Stream.of(
                Arguments.of("", Set.of(), Map.of()),
                Arguments.of(" ", Set.of(), Map.of()),
                Arguments.of(null, Set.of(), Map.of()),
                Arguments.of(null, Set.of("topic1"), Map.of()),
                Arguments.of("0:5,1:6,2:7,3:8", Set.of("topic1"), Map.of(
                        tp("topic1", 0), Optional.of(5L),
                        tp("topic1", 1), Optional.of(6L),
                        tp("topic1", 2), Optional.of(7L),
                        tp("topic1", 3), Optional.of(8L))),
                Arguments.of("1,2", Set.of("topic1"), Map.of(
                        tp("topic1", 1), Optional.empty(),
                        tp("topic1", 2), Optional.empty())),
                Arguments.of("0", Set.of("topic1"), Map.of(
                        tp("topic1", 0), Optional.empty())),
                Arguments.of("0:-1", Set.of("topic1"), Map.of(
                        tp("topic1", 0), Optional.of(-1L))),
                Arguments.of("0:0", Set.of("topic1"), Map.of(
                        tp("topic1", 0), Optional.of(0L))),
                Arguments.of("topic1:0,topic2:0", Set.of("topic3"), Map.of(
                        tp("topic1", 0), Optional.empty(),
                        tp("topic2", 0), Optional.empty())),
                Arguments.of("topic1:0:5,topic1:1:6,topic2:0:7,topic2:1:8", Set.of("topic1", "topic2"), Map.of(
                        tp("topic1", 0), Optional.of(5L),
                        tp("topic1", 1), Optional.of(6L),
                        tp("topic2", 0), Optional.of(7L),
                        tp("topic2", 1), Optional.of(8L))),
                Arguments.of("topic1:0:5, topic1:1:6, topic2:0:7, topic2:1:8", Set.of(), Map.of(
                        tp("topic1", 0), Optional.of(5L),
                        tp("topic1", 1), Optional.of(6L),
                        tp("topic2", 0), Optional.of(7L),
                        tp("topic2", 1), Optional.of(8L))),
                Arguments.of("topic1:0:5,topic1:1:6,topic2:0:7,topic2:1:8", Set.of(), Map.of(
                        tp("topic1", 0), Optional.of(5L),
                        tp("topic1", 1), Optional.of(6L),
                        tp("topic2", 0), Optional.of(7L),
                        tp("topic2", 1), Optional.of(8L))));
    }

    static Stream<Arguments> checkThrows() {
        return Stream.of(
                Arguments.of("abcd", Set.of("topic1", "topic2"), (ThrowingConsumer<? super Throwable>) (t) -> assertThat(t)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("`abcd`", "`assign-seek`")),
                Arguments.of("abcd", Set.of(), (ThrowingConsumer<? super Throwable>) (t) -> assertThat(t)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("`abcd`", "`assign-seek`")),
                Arguments.of("topic1:abcd", Set.of(), (ThrowingConsumer<? super Throwable>) (t) -> assertThat(t)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("`topic1:abcd`", "`assign-seek`")
                        .hasCauseExactlyInstanceOf(NumberFormatException.class)),
                Arguments.of("0:abcd", Set.of(), (ThrowingConsumer<? super Throwable>) (t) -> assertThat(t)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("`0:abcd`", "`assign-seek`")
                        .hasCauseExactlyInstanceOf(NumberFormatException.class)),
                Arguments.of("0:5,1:6,2:7,3:8", Set.of(), (ThrowingConsumer<? super Throwable>) (t) -> assertThat(t)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("`0:5`", "`assign-seek`")),
                Arguments.of("1:6", Set.of("topic1", "topic2"), (ThrowingConsumer<? super Throwable>) (t) -> assertThat(t)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("`1:6`", "`assign-seek`")),
                Arguments.of("0", Set.of(), (ThrowingConsumer<? super Throwable>) (t) -> assertThat(t)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("`0`", "`assign-seek`")),
                Arguments.of("0", Set.of("topic1", "topic2"), (ThrowingConsumer<? super Throwable>) (t) -> assertThat(t)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContainingAll("`0`", "`assign-seek`")
                        .hasCauseExactlyInstanceOf(IllegalArgumentException.class)));
    }

    @ParameterizedTest
    @MethodSource("checkOffsets")
    void testOffsets(String offsetSeeks, Set<String> topics, Map<TopicPartition, Optional<Long>> expected) {
        assertThat(KafkaSource.getOffsetSeeks(offsetSeeks, "channel", topics)).containsAllEntriesOf(expected);
    }

    @ParameterizedTest
    @MethodSource("checkThrows")
    void testExpectedThrows(String offsetSeeks, Set<String> topics, ThrowingConsumer<? super Throwable> assertions) {
        assertThatThrownBy(() -> KafkaSource.getOffsetSeeks(offsetSeeks, "channel", topics))
                .satisfies(assertions);
    }
}
