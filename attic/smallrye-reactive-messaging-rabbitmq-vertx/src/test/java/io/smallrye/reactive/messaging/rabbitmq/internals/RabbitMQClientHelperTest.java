package io.smallrye.reactive.messaging.rabbitmq.internals;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Address;

import io.vertx.rabbitmq.RabbitMQOptions;

class RabbitMQClientHelperTest {

    @Test
    void testIdenticalOptionsProduceSameFingerprint() {
        RabbitMQOptions options1 = new RabbitMQOptions()
                .setHost("localhost")
                .setPort(5672)
                .setUser("guest")
                .setPassword("guest")
                .setVirtualHost("/");

        RabbitMQOptions options2 = new RabbitMQOptions()
                .setHost("localhost")
                .setPort(5672)
                .setUser("guest")
                .setPassword("guest")
                .setVirtualHost("/");

        String fingerprint1 = RabbitMQClientHelper.computeConnectionFingerprint(options1);
        String fingerprint2 = RabbitMQClientHelper.computeConnectionFingerprint(options2);

        assertThat(fingerprint1).isEqualTo(fingerprint2);
    }

    @Test
    void testDifferentHostsProduceDifferentFingerprints() {
        RabbitMQOptions options1 = new RabbitMQOptions().setHost("host-a").setPort(5672);
        RabbitMQOptions options2 = new RabbitMQOptions().setHost("host-b").setPort(5672);

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(options1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(options2));
    }

    @Test
    void testDifferentAddressesProduceDifferentFingerprints() {
        RabbitMQOptions options1 = new RabbitMQOptions().setAddresses(List.of(new Address("host-a", 5672)));
        RabbitMQOptions options2 = new RabbitMQOptions().setAddresses(List.of(new Address("host-a", 5673)));

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(options1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(options2));
    }

    @Test
    void testDifferentPortsProduceDifferentFingerprints() {
        RabbitMQOptions options1 = new RabbitMQOptions().setHost("localhost").setPort(5672);
        RabbitMQOptions options2 = new RabbitMQOptions().setHost("localhost").setPort(5673);

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(options1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(options2));
    }

    @Test
    void testDifferentUsersProduceDifferentFingerprints() {
        RabbitMQOptions options1 = new RabbitMQOptions().setHost("localhost").setUser("alice");
        RabbitMQOptions options2 = new RabbitMQOptions().setHost("localhost").setUser("bob");

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(options1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(options2));
    }

    @Test
    void testDifferentVirtualHostsProduceDifferentFingerprints() {
        RabbitMQOptions options1 = new RabbitMQOptions().setHost("localhost").setVirtualHost("/");
        RabbitMQOptions options2 = new RabbitMQOptions().setHost("localhost").setVirtualHost("/staging");

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(options1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(options2));
    }

    @Test
    void testDifferentSslProduceDifferentFingerprints() {
        RabbitMQOptions options1 = new RabbitMQOptions().setHost("localhost").setSsl(false);
        RabbitMQOptions options2 = new RabbitMQOptions().setHost("localhost").setSsl(true);

        assertThat(RabbitMQClientHelper.computeConnectionFingerprint(options1))
                .isNotEqualTo(RabbitMQClientHelper.computeConnectionFingerprint(options2));
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

    // --- computeConnectionFingerprint determinism ---

    @Test
    void testFingerprintIsDeterministic() {
        RabbitMQOptions options = new RabbitMQOptions()
                .setHost("localhost")
                .setPort(5672)
                .setUser("guest");

        String first = RabbitMQClientHelper.computeConnectionFingerprint(options);
        String second = RabbitMQClientHelper.computeConnectionFingerprint(options);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void testFingerprintWithNullAddresses() {
        RabbitMQOptions options = new RabbitMQOptions().setHost("localhost");
        // No addresses set → should still compute fingerprint without errors
        String fingerprint = RabbitMQClientHelper.computeConnectionFingerprint(options);
        assertThat(fingerprint).isNotEmpty();
    }

    @Test
    void testFingerprintIsHexSha256() {
        RabbitMQOptions options = new RabbitMQOptions().setHost("localhost");
        String fingerprint = RabbitMQClientHelper.computeConnectionFingerprint(options);

        // SHA-256 produces 64-character hex string
        assertThat(fingerprint).hasSize(64);
        assertThat(fingerprint).matches("[0-9a-f]+");
    }

}
