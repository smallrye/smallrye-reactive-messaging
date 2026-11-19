package io.smallrye.reactive.messaging.rabbitmq;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.vertx.mutiny.core.Vertx;

/**
 * Tests for the case where the broker refuses to hand out more channels.
 * <p>
 * The RabbitMQ client returns {@code null} from {@link Connection#createChannel()} once the negotiated channel-max is
 * reached. The {@link ConnectionHolder} must report that as an error explaining the limit, not as the bare
 * {@link NullPointerException} that storing the {@code null} in a {@link java.util.concurrent.ConcurrentHashMap} used to
 * produce.
 */
public class ConnectionHolderChannelMaxTest {

    private Vertx vertx;
    private ConnectionHolder holder;

    @BeforeEach
    public void setUp() throws Exception {
        vertx = Vertx.vertx();

        ConnectionFactory factory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(factory.newConnection(anyString())).thenReturn(connection);
        when(connection.isOpen()).thenReturn(true);
        when(connection.getChannelMax()).thenReturn(1);
        // The client returns null when the channel-max negotiated with the broker has been reached
        when(connection.createChannel()).thenReturn(null);

        holder = new ConnectionHolder(factory, "test-channel", vertx);
        holder.connect().await().indefinitely();
    }

    @AfterEach
    public void tearDown() {
        if (holder != null) {
            holder.close();
        }
        if (vertx != null) {
            vertx.closeAndAwait();
        }
    }

    @Test
    public void testCreateChannelReportsChannelMaxReached() {
        assertThatThrownBy(() -> holder.createChannel())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("SRMSG19014: ")
                .hasMessageContaining("channel max (1) reached")
                .hasMessageContaining("requestedChannelMax");
    }

    @Test
    public void testGetOrCreateSharedChannelReportsChannelMaxReached() {
        assertThatThrownBy(() -> holder.getOrCreateSharedChannel("shared"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("`shared`")
                .hasMessageContaining("channel max (1) reached");
    }
}
