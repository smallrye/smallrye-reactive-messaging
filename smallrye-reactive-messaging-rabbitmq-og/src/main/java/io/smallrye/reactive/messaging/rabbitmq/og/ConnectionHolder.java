package io.smallrye.reactive.messaging.rabbitmq.og;

import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQLogging.log;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private final String connectionName;
    private final Vertx vertx;
    private final Context context;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final int reconnectAttempts;
    private final int reconnectInterval;
    private final Set<String> channels = ConcurrentHashMap.newKeySet();

    private volatile Connection connection;
    private final ConcurrentHashMap<String, Channel> sharedChannels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Context> sharedChannelContexts = new ConcurrentHashMap<>();
    private volatile Uni<Connection> connectionUni;
    private Consumer<Connection> onConnectionEstablished;

    public ConnectionHolder(
            ConnectionFactory factory,
            String channelName,
            io.vertx.mutiny.core.Vertx mutinyVertx) {
        this(factory, channelName, channelName, mutinyVertx, 0, 10);
    }

    public ConnectionHolder(
            ConnectionFactory factory,
            String channelName,
            String connectionName,
            io.vertx.mutiny.core.Vertx mutinyVertx,
            int reconnectAttempts,
            int reconnectInterval) {
        this.factory = factory;
        this.channelName = channelName;
        this.connectionName = connectionName;
        this.vertx = mutinyVertx.getDelegate();
        this.reconnectAttempts = reconnectAttempts;
        this.reconnectInterval = reconnectInterval;
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
        if (connectionUni != null) {
            return connectionUni;
        }
        synchronized (this) {
            if (connectionUni != null) {
                return connectionUni;
            }
            Uni<Connection> uni = Uni.createFrom().item(() -> {
                try {
                    log.establishingConnection(channelName);

                    Connection conn = factory.newConnection(connectionName);

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
                uni = uni
                        .onFailure().retry()
                        .withBackOff(Duration.ofSeconds(reconnectInterval))
                        .atMost(reconnectAttempts);
            }

            connectionUni = uni.memoize().until(() -> connection != null && !connection.isOpen());
            return connectionUni;
        }
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
     * Returns the Vert.x context for the given shared channel name, creating it if needed.
     * All access to the corresponding shared AMQP channel must be serialized through this context.
     */
    public Context getOrCreateSharedChannelContext(String name) {
        return sharedChannelContexts.computeIfAbsent(name,
                k -> ((VertxInternal) vertx).createEventLoopContext());
    }

    /**
     * Returns the shared AMQP channel for the given name, creating it if needed.
     * An outgoing publisher and its reply-to consumer must use the same named channel
     * for RabbitMQ direct reply-to to work (it is channel-scoped).
     * All access must be serialized through {@link #getOrCreateSharedChannelContext(String)}.
     */
    public Channel getOrCreateSharedChannel(String name) throws IOException {
        Channel ch = sharedChannels.get(name);
        if (ch == null || !ch.isOpen()) {
            if (connection == null || !connection.isOpen()) {
                throw ex.illegalStateConnectionClosed();
            }
            ch = connection.createChannel();
            sharedChannels.put(name, ch);
        }
        return ch;
    }

    /**
     * Creates a new private channel.
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

    public Set<String> channels() {
        return channels;
    }

    public ConnectionHolder retain(String channel) {
        channels.add(channel);
        return this;
    }

    public boolean release(String channel) {
        channels.remove(channel);
        return channels.isEmpty();
    }
}
