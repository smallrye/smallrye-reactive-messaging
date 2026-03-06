package io.smallrye.reactive.messaging.rabbitmq.og;

import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQLogging.log;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.rabbitmq.client.*;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;

/**
 * Manages a RabbitMQ connection and provides channels.
 * Handles automatic recovery and ensures topology setup happens before consumers start.
 */
public class ConnectionHolder {

    private final ConnectionFactory factory;
    private final String channelName;
    private final Vertx vertx;
    private final Context context;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final int reconnectAttempts;
    private final int reconnectInterval;

    private volatile Connection connection;
    private Consumer<Connection> onConnectionEstablished;

    public ConnectionHolder(
            ConnectionFactory factory,
            String channelName,
            io.vertx.mutiny.core.Vertx mutinyVertx) {
        this(factory, channelName, mutinyVertx, 0, 10);
    }

    public ConnectionHolder(
            ConnectionFactory factory,
            String channelName,
            io.vertx.mutiny.core.Vertx mutinyVertx,
            int reconnectAttempts,
            int reconnectInterval) {
        this.factory = factory;
        this.channelName = channelName;
        this.vertx = mutinyVertx.getDelegate();
        this.reconnectAttempts = reconnectAttempts;
        this.reconnectInterval = reconnectInterval;
        // We need event loop context for each connection, so the duplicated context will be running on different event
        // loop threads, but that's fine as long as we use the same context for all operations related to this connection
        this.context = ((VertxInternal) vertx).createEventLoopContext();
    }

    /**
     * Sets a callback to be invoked when the connection is established.
     * This is where topology (queues, exchanges) should be set up.
     */
    public void onConnectionEstablished(Consumer<Connection> callback) {
        this.onConnectionEstablished = callback;
    }

    /**
     * Connects to RabbitMQ broker and sets up recovery listeners.
     * Retries connection based on reconnect-attempts and reconnect-interval configuration.
     */
    public Uni<Connection> connect() {
        Uni<Connection> connectionUni = Uni.createFrom().item(() -> {
            try {
                log.establishingConnection(channelName);

                Connection conn = factory.newConnection();

                // Set connection before recovery listeners so createChannel() works in recovery callbacks
                connection = conn;
                connected.set(true);

                setupRecoveryListeners(conn);

                log.connectionEstablished(channelName);

                return conn;
            } catch (IOException | TimeoutException e) {
                log.unableToConnectToBroker(e);
                throw ex.illegalStateUnableToCreateClient(e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());

        if (reconnectAttempts > 0) {
            connectionUni = connectionUni
                    .onFailure().retry()
                    .withBackOff(Duration.ofSeconds(reconnectInterval))
                    .atMost(reconnectAttempts);
        }

        return connectionUni;
    }

    private void setupRecoveryListeners(Connection conn) {
        if (conn instanceof Recoverable) {
            Recoverable recoverable = (Recoverable) conn;

            recoverable.addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                    log.establishingConnection(channelName);
                }

                @Override
                public void handleRecovery(Recoverable recoverable) {
                    // Connection has been re-established
                    // Run topology setup before consumers are restarted
                    if (onConnectionEstablished != null) {
                        try {
                            onConnectionEstablished.accept((Connection) recoverable);
                            log.connectionRecovered(channelName);
                        } catch (Exception e) {
                            log.unableToRecoverFromConnectionDisruption(e);
                        }
                    } else {
                        log.connectionRecovered(channelName);
                    }
                }
            });
        }
    }

    /**
     * Creates a new channel.
     * Note: Channels are NOT thread-safe and should be used from a single thread or synchronized.
     */
    public Channel createChannel() throws IOException {
        if (connection == null || !connection.isOpen()) {
            throw ex.illegalStateConnectionClosed();
        }

        try {
            log.creatingChannel(channelName);
            return connection.createChannel();
        } catch (IOException e) {
            log.unableToCreateClient(e);
            throw ex.illegalStateUnableToCreateChannel(e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public boolean isConnected() {
        return connection != null && connection.isOpen();
    }

    public boolean hasBeenConnected() {
        return connected.get();
    }

    public Context getContext() {
        return context;
    }

    public Vertx getVertx() {
        return vertx;
    }

    public io.vertx.mutiny.core.Vertx getMutinyVertx() {
        return io.vertx.mutiny.core.Vertx.newInstance(vertx);
    }

    public void close() {
        Connection conn = connection;
        if (conn != null && conn.isOpen()) {
            try {
                conn.close();
            } catch (IOException e) {
                // Ignore close errors
            }
        }
    }
}
