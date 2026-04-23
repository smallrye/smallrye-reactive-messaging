package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static java.time.Duration.ofSeconds;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.qpid.proton.amqp.Symbol;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.cbs.CbsExchange;
import io.smallrye.reactive.messaging.amqp.cbs.CbsTokenManager;
import io.smallrye.reactive.messaging.amqp.cbs.RefreshingCbsTokenManager;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class ConnectionHolder {

    private final AmqpConnectorCommonConfiguration configuration;
    private final AtomicReference<CurrentConnection> holder = new AtomicReference<>();

    private final Vertx vertx;
    private final Context root;
    private final Uni<AmqpConnection> connection;
    private Consumer<Throwable> onFailure;

    public ConnectionHolder(AmqpClient client,
            CbsTokenManager baseTokenManager,
            AmqpConnectorCommonConfiguration configuration,
            Vertx vertx, Context root) {
        CbsTokenManager tokenManager = baseTokenManager == null ? null
                : new RefreshingCbsTokenManager(baseTokenManager, vertx, root, this::closeCurrent);
        this.configuration = configuration;
        this.vertx = vertx;
        this.root = root;

        Integer retryInterval = configuration.getReconnectInterval();
        Integer retryAttempts = configuration.getReconnectAttempts();

        this.connection = Uni.createFrom().deferred(() -> client.connect()
                .runSubscriptionOn(root::runOnContext)
                .onSubscription().invoke(s -> log.establishingConnection())
                .onFailure().invoke(log::unableToConnectToBroker)
                .onItem().invoke(log::connectionEstablished)
                .onItem().transform(conn -> {
                    CbsExchange cbsExchange = tokenManager == null ? null : tokenManager.exchange(conn);
                    return new CurrentConnection(conn, cbsExchange);
                })
                .onItem().transformToUni(currentConnection -> {
                    if (tokenManager != null && currentConnection.exchange() != null) {
                        return tokenManager.authorize(currentConnection.exchange())
                                .onFailure().invoke(t -> currentConnection.close())
                                .replaceWith(currentConnection);
                    } else {
                        return Uni.createFrom().item(currentConnection);
                    }
                })
                .onItem().transform(currentConn -> {
                    holder.set(currentConn);
                    AmqpConnection conn = currentConn.connection;
                    conn
                            .exceptionHandler(t -> {
                                closeCurrent();
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
                        closeCurrent();
                        throw ex.illegalStateConnectionDisconnected();
                    }
                    return conn;
                })
                .onFailure().invoke(log::unableToConnectToBroker)
                .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(retryInterval)).atMost(retryAttempts)
                .onFailure().invoke(t -> {
                    holder.set(null);
                    log.unableToRecoverFromConnectionDisruption(t);
                }))
                .memoize().until(() -> {
                    CurrentConnection current = holder.get();
                    return current == null || current.connection == null || current.connection.isDisconnected();
                });
    }

    void closeCurrent() {
        CurrentConnection current = holder.getAndSet(null);
        if (current != null) {
            current.close();
        }
    }

    public Context getContext() {
        return root;
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
                .runSubscriptionOn(root::runOnContext);
    }

    /**
     * Retrieves the underlying connection capabilities.
     * Must be called from the appropriate context.
     *
     * @return the list of capability
     */
    public static List<String> capabilities(AmqpConnection connection) {
        Symbol[] capabilities = connection.getDelegate().unwrap().getRemoteOfferedCapabilities();
        if (capabilities == null) {
            return List.of();
        }
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

    private record CurrentConnection(AmqpConnection connection, CbsExchange exchange) {

        public void close() {
            if (exchange != null) {
                exchange.close();
            }
        }
    }

    public synchronized void onFailure(Consumer<Throwable> callback) {
        this.onFailure = callback;
    }

    /**
     * Safe to call from any thread: memoize().until() handles concurrent subscriptions,
     * ensuring a single in-flight connection attempt is shared across all callers.
     */
    @CheckReturnValue
    public Uni<AmqpConnection> getOrEstablishConnection() {
        return connection;
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

}
