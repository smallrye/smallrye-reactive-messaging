package io.smallrye.reactive.messaging.rabbitmq.internals;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

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

}
