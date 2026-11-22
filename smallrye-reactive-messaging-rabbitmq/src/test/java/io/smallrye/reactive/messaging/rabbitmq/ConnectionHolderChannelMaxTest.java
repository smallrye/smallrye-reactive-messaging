package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.vertx.mutiny.core.Vertx;

/**
 * Tests the failure when the broker connection cannot hand out another channel because the
 * negotiated channel max has been reached. {@code Connection#createChannel()} returns
 * {@code null} in that case, which used to surface as a bare {@code NullPointerException}
 * from the shared-channel map. No broker is needed: the connection is stubbed.
 */
public class ConnectionHolderChannelMaxTest {

    @Test
    public void testCreateChannelFailsWithClearErrorWhenChannelMaxReached() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionHolder holder = new ConnectionHolder(stubFactory(), "test-channel", vertx);
            holder.connect().await().indefinitely();

            assertThatThrownBy(holder::createChannel)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("channel max (1) reached");
        } finally {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testSharedChannelFailsWithClearErrorWhenChannelMaxReached() {
        Vertx vertx = Vertx.vertx();
        try {
            ConnectionHolder holder = new ConnectionHolder(stubFactory(), "test-channel", vertx);
            holder.connect().await().indefinitely();

            assertThatThrownBy(() -> holder.getOrCreateSharedChannel("test-channel"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("channel max (1) reached");
        } finally {
            vertx.closeAndAwait();
        }
    }

    private static ConnectionFactory stubFactory() {
        Connection connection = stubConnection();
        return new ConnectionFactory() {
            @Override
            public Connection newConnection(String connectionName) {
                return connection;
            }
        };
    }

    private static Connection stubConnection() {
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "isOpen":
                    return true;
                case "createChannel":
                    // No channel number available: the negotiated channel max has been reached.
                    return null;
                case "getChannelMax":
                    return 1;
                case "close":
                    return null;
                default:
                    return defaultValue(method.getReturnType());
            }
        };
        return (Connection) Proxy.newProxyInstance(
                ConnectionHolderChannelMaxTest.class.getClassLoader(),
                new Class<?>[] { Connection.class },
                handler);
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0.0f;
        }
        if (type == double.class) {
            return 0.0d;
        }
        return null;
    }
}
