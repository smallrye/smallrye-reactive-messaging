package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.vertx.core.Context;
import io.vertx.mutiny.core.Vertx;

/**
 * Tests for the ConnectionHolder class (Phase 2.1: Connection Management)
 */
public class ConnectionHolderTest extends RabbitMQBrokerTestBase {

    @Test
    public void testBasicConnectionEstablishment() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            Connection connection = holder.connect().await().indefinitely();

            assertThat(connection).isNotNull();
            assertThat(connection.isOpen()).isTrue();
            assertThat(holder.isConnected()).isTrue();
            assertThat(holder.hasBeenConnected()).isTrue();

            holder.close();
            assertThat(holder.isConnected()).isFalse();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testConnectionEstablishedCallback() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            AtomicBoolean callbackInvoked = new AtomicBoolean(false);
            AtomicInteger callbackCount = new AtomicInteger(0);

            holder.onConnectionEstablished(conn -> {
                assertThat(conn).isNotNull();
                assertThat(conn.isOpen()).isTrue();
                callbackInvoked.set(true);
                callbackCount.incrementAndGet();
            });

            holder.connect().await().indefinitely();

            // The onConnectionEstablished callback is only invoked during recovery,
            // not on the initial connection (initial setup is handled by the connect subscriber)
            assertThat(callbackInvoked.get()).isFalse();
            assertThat(callbackCount.get()).isEqualTo(0);

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testCreateChannel() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            holder.connect().await().indefinitely();

            Channel channel = holder.createChannel();
            assertThat(channel).isNotNull();
            assertThat(channel.isOpen()).isTrue();

            channel.close();
            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testCreateMultipleChannels() throws Exception {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            holder.connect().await().indefinitely();

            // Create multiple channels from the same connection
            Channel channel1 = holder.createChannel();
            Channel channel2 = holder.createChannel();
            Channel channel3 = holder.createChannel();

            assertThat(channel1).isNotNull();
            assertThat(channel2).isNotNull();
            assertThat(channel3).isNotNull();

            assertThat(channel1.isOpen()).isTrue();
            assertThat(channel2.isOpen()).isTrue();
            assertThat(channel3.isOpen()).isTrue();

            channel1.close();
            channel2.close();
            channel3.close();
            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testCreateChannelBeforeConnection() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            // Try to create a channel without connecting first
            assertThatThrownBy(() -> holder.createChannel())
                    .isInstanceOf(IllegalStateException.class);

        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testVertxContextIntegration() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            // Create a duplicated context
            Context context = vertx.getOrCreateContext().getDelegate();

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            assertThat(holder.getContext()).isNotNull();
            assertThat(holder.getVertx()).isNotNull();
            assertThat(holder.getVertx()).isEqualTo(vertx.getDelegate());

            holder.connect().await().indefinitely();
            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testConnectionFactoryWithAutomaticRecovery() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setAutomaticRecoveryEnabled(true);
            factory.setNetworkRecoveryInterval(1000);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            Connection connection = holder.connect().await().indefinitely();

            assertThat(connection).isNotNull();
            assertThat(connection.isOpen()).isTrue();

            holder.close();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testInvalidCredentials() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername("invalid-user");
            factory.setPassword("invalid-password");

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            assertThatThrownBy(() -> holder.connect().await().indefinitely())
                    .hasRootCauseInstanceOf(IOException.class);

            assertThat(holder.isConnected()).isFalse();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testInvalidHost() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("invalid-host-that-does-not-exist");
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setConnectionTimeout(2000);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            assertThatThrownBy(() -> holder.connect().await().indefinitely())
                    .hasRootCauseInstanceOf(IOException.class);

            assertThat(holder.isConnected()).isFalse();
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testCloseConnectionIdempotent() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            ConnectionHolder holder = new ConnectionHolder(factory, "test-channel", vertx);

            holder.connect().await().indefinitely();
            assertThat(holder.isConnected()).isTrue();

            holder.close();
            assertThat(holder.isConnected()).isFalse();

            // Closing again should not throw
            holder.close();
            assertThat(holder.isConnected()).isFalse();
        } finally {
            vertx.closeAndAwait();
        }
    }
}
