package io.smallrye.reactive.messaging.rabbitmq.og.internals;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.ConnectionFactory;

class RabbitMQClientHelperTest {

    // --- computeConnectionFingerprint ---

    @Test
    void testIdenticalOptionsProduceSameFingerprint() {
        ConnectionFactory factory1 = new ConnectionFactory();
        factory1.setHost("localhost");
        factory1.setPort(5672);
        factory1.setUsername("guest");
        factory1.setPassword("guest");
        factory1.setVirtualHost("/");

        ConnectionFactory factory2 = new ConnectionFactory();
        factory2.setHost("localhost");
        factory2.setPort(5672);
        factory2.setUsername("guest");
        factory2.setPassword("guest");
        factory2.setVirtualHost("/");

        String fingerprint1 = RabbitMQClientHelper.computeConnectionFingerprint(factory1);
        String fingerprint2 = RabbitMQClientHelper.computeConnectionFingerprint(factory2);

        assertThat(fingerprint1).isEqualTo(fingerprint2);
    }

    @Test
    void testDifferentHostsProduceDifferentFingerprints() {
        ConnectionFactory factory1 = new ConnectionFactory();
        factory1.setHost("host-a");
        factory1.setPort(5672);

        ConnectionFactory factory2 = new ConnectionFactory();
        factory2.setHost("host-b");
        factory2.setPort(5672);

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(factory1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(factory2));
    }

    @Test
    void testDifferentPortsProduceDifferentFingerprints() {
        ConnectionFactory factory1 = new ConnectionFactory();
        factory1.setHost("localhost");
        factory1.setPort(5672);

        ConnectionFactory factory2 = new ConnectionFactory();
        factory2.setHost("localhost");
        factory2.setPort(5673);

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(factory1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(factory2));
    }

    @Test
    void testDifferentUsersProduceDifferentFingerprints() {
        ConnectionFactory factory1 = new ConnectionFactory();
        factory1.setHost("localhost");
        factory1.setUsername("alice");

        ConnectionFactory factory2 = new ConnectionFactory();
        factory2.setHost("localhost");
        factory2.setUsername("bob");

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(factory1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(factory2));
    }

    @Test
    void testDifferentVirtualHostsProduceDifferentFingerprints() {
        ConnectionFactory factory1 = new ConnectionFactory();
        factory1.setHost("localhost");
        factory1.setVirtualHost("/");

        ConnectionFactory factory2 = new ConnectionFactory();
        factory2.setHost("localhost");
        factory2.setVirtualHost("/staging");

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(factory1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(factory2));
    }

    @Test
    void testDifferentSslProduceDifferentFingerprints() throws Exception {
        ConnectionFactory factory1 = new ConnectionFactory();
        factory1.setHost("localhost");

        ConnectionFactory factory2 = new ConnectionFactory();
        factory2.setHost("localhost");
        factory2.useSslProtocol();

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(factory1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(factory2));
    }

    @Test
    void testFingerprintIsDeterministic() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");

        String first = RabbitMQClientHelper.computeConnectionFingerprint(factory);
        String second = RabbitMQClientHelper.computeConnectionFingerprint(factory);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void testFingerprintIsHexSha256() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        String fingerprint = RabbitMQClientHelper.computeConnectionFingerprint(factory);

        assertThat(fingerprint).hasSize(64);
        assertThat(fingerprint).matches("[0-9a-f]+");
    }

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
