package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static java.time.Duration.ofSeconds;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.qpid.proton.amqp.Symbol;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class ConnectionHolder {

    private final AmqpClient client;
    private final AmqpConnectorCommonConfiguration configuration;
    private final AtomicReference<CurrentConnection> holder = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<AmqpConnection>> ongoingConnection = new AtomicReference<>();

    private final Vertx vertx;
    private final Context root;
    private Consumer<Throwable> onFailure;

    public ConnectionHolder(AmqpClient client,
            AmqpConnectorCommonConfiguration configuration,
            Vertx vertx, Context root) {
        this.client = client;
        this.configuration = configuration;
        this.vertx = vertx;
        this.root = root;
    }

    public Context getContext() {
        CurrentConnection connection = holder.get();
        if (connection != null) {
            return connection.context;
        } else {
            return null;
        }
    }

    @CheckReturnValue
    public Uni<Boolean> isConnected() {
        CurrentConnection connection = holder.get();
        if (connection == null) {
            return Uni.createFrom().item(false);
        }

        AmqpConnection underlying = connection.connection;
        if (underlying == null) {
            return Uni.createFrom().item(false);
        }

        return Uni.createFrom().item(() -> !underlying.isDisconnected())
                .runSubscriptionOn(connection.context::runOnContext);
    }

    /**
     * Retrieves the underlying connection capabilities.
     * Must be called from the appropriate context.
     *
     * @return the list of capability
     */
    public static List<String> capabilities(AmqpConnection connection) {
        Symbol[] capabilities = connection.getDelegate().unwrap().getRemoteOfferedCapabilities();
        return Arrays.stream(capabilities).map(Symbol::toString).collect(Collectors.toList());
    }

    /**
     * Checks whether the given connection support anonymous relay (and so can create an anonymous sender).
     * Must be called from the appropriate context.
     *
     * @param connection the connection
     * @return true if the connection offers the anynymous relay capability
     */
    public static boolean supportAnonymousRelay(AmqpConnection connection) {
        return capabilities(connection).contains("ANONYMOUS-RELAY");
    }

    public Vertx getVertx() {
        return vertx;
    }

    public int getHealthTimeout() {
        return configuration.getHealthTimeout();
    }

    private record CurrentConnection(AmqpConnection connection, Context context) {
    }

    public synchronized void onFailure(Consumer<Throwable> callback) {
        this.onFailure = callback;
    }

    @CheckReturnValue
    public Uni<AmqpConnection> getOrEstablishConnection() {
        return Uni.createFrom().deferred(this::establishConnection);
    }

    private Uni<AmqpConnection> establishConnection() {
        CurrentConnection current = holder.get();
        if (current != null && current.connection != null
                && !current.connection.isDisconnected()) {
            return Uni.createFrom().item(current.connection);
        }

        CompletableFuture<AmqpConnection> existing = ongoingConnection.get();
        if (existing != null) {
            if (!existing.isDone() || (current != null && !current.connection.isDisconnected())) {
                return Uni.createFrom().completionStage(existing);
            }
            ongoingConnection.compareAndSet(existing, null);
        }

        CompletableFuture<AmqpConnection> placeholder = new CompletableFuture<>();
        CompletableFuture<AmqpConnection> prev = ongoingConnection.compareAndExchange(null, placeholder);
        if (prev != null) {
            return Uni.createFrom().completionStage(prev);
        }

        Integer retryInterval = configuration.getReconnectInterval();
        Integer retryAttempts = configuration.getReconnectAttempts();

        client.connect()
                .runSubscriptionOn(root::runOnContext)
                .onSubscription().invoke(s -> log.establishingConnection())
                .onFailure().invoke(log::unableToConnectToBroker)
                .onItem().invoke(log::connectionEstablished)
                .onItem().transform(conn -> new CurrentConnection(conn, root))
                .onItem().transform(currentConn -> {
                    holder.set(currentConn);
                    AmqpConnection conn = currentConn.connection;
                    conn
                            .exceptionHandler(t -> {
                                holder.set(null);
                                log.connectionFailure(t);

                                Consumer<Throwable> c;
                                synchronized (this) {
                                    c = onFailure;
                                }
                                if (c != null) {
                                    c.accept(t);
                                }
                            });
                    if (conn.isDisconnected() || holder.get() == null) {
                        holder.set(null);
                        throw ex.illegalStateConnectionDisconnected();
                    }
                    return conn;
                })
                .onFailure().invoke(log::unableToConnectToBroker)
                .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(retryInterval)).atMost(retryAttempts)
                .onFailure().invoke(t -> {
                    holder.set(null);
                    log.unableToRecoverFromConnectionDisruption(t);
                })
                .subscribe().with(placeholder::complete, t -> {
                    ongoingConnection.compareAndSet(placeholder, null);
                    placeholder.completeExceptionally(t);
                });

        return Uni.createFrom().completionStage(placeholder);
    }

    public static CompletionStage<Void> runOnContext(Context context, AmqpMessage<?> msg,
            Consumer<io.vertx.mutiny.amqp.AmqpMessage> handle) {
        return VertxContext.runOnContext(context.getDelegate(), f -> {
            handle.accept(msg.getAmqpMessage());
            msg.runOnMessageContext(() -> f.complete(null));
        });
    }

    public static CompletionStage<Void> runOnContextAndReportFailure(Context context, AmqpMessage<?> msg,
            Throwable reason, Consumer<io.vertx.mutiny.amqp.AmqpMessage> handle) {
        return VertxContext.runOnContext(context.getDelegate(), f -> {
            handle.accept(msg.getAmqpMessage());
            msg.runOnMessageContext(() -> f.completeExceptionally(reason));
        });
    }

    void runWithTrampoline(Runnable action) {
        Context context = getContext();
        if (context != null) {
            VertxContext.runOnContext(context.getDelegate(), action);
        } else {
            root.runOnContext(action);
        }
    }
}
