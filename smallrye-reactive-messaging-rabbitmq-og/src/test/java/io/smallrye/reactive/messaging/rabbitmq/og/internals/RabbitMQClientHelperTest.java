package io.smallrye.reactive.messaging.rabbitmq.og.internals;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class RabbitMQClientHelperTest {

    // --- serverQueueName ---

    @Test
    void testServerQueueNameWithServerAuto() {
        assertThat(RabbitMQClientHelper.serverQueueName("(server.auto)")).isEmpty();
    }

    @Test
    void testServerQueueNameWithRegularName() {
        assertThat(RabbitMQClientHelper.serverQueueName("my-queue")).isEqualTo("my-queue");
    }

    @Test
    void testServerQueueNameWithEmptyString() {
        assertThat(RabbitMQClientHelper.serverQueueName("")).isEmpty();
    }

    // --- parseArguments ---

    @Test
    void testParseArgumentsWithEmpty() {
        Map<String, Object> result = RabbitMQClientHelper.parseArguments(Optional.empty());
        assertThat(result).isEmpty();
    }

    @Test
    void testParseArgumentsWithSingleStringArgument() {
        Map<String, Object> result = RabbitMQClientHelper.parseArguments(Optional.of("x-queue-type:quorum"));
        assertThat(result).containsEntry("x-queue-type", "quorum");
    }

    @Test
    void testParseArgumentsWithSingleIntegerArgument() {
        Map<String, Object> result = RabbitMQClientHelper.parseArguments(Optional.of("x-priority:10"));
        assertThat(result).containsEntry("x-priority", 10);
    }

    @Test
    void testParseArgumentsWithMultipleArguments() {
        Map<String, Object> result = RabbitMQClientHelper.parseArguments(
                Optional.of("x-priority:10,x-queue-type:quorum"));
        assertThat(result)
                .containsEntry("x-priority", 10)
                .containsEntry("x-queue-type", "quorum")
                .hasSize(2);
    }

    @Test
    void testParseArgumentsWithSpaces() {
        Map<String, Object> result = RabbitMQClientHelper.parseArguments(
                Optional.of(" x-priority:5 , x-queue-type:classic "));
        assertThat(result)
                .containsEntry("x-priority", 5)
                .containsEntry("x-queue-type", "classic");
    }

    @Test
    void testParseArgumentsIgnoresMalformedSegments() {
        // Segments without ":" separator or with multiple ":" are skipped (length != 2)
        Map<String, Object> result = RabbitMQClientHelper.parseArguments(
                Optional.of("no-colon,valid-key:valid-value"));
        assertThat(result)
                .containsEntry("valid-key", "valid-value")
                .hasSize(1);
    }

}
