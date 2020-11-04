package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static java.time.Duration.ofSeconds;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class ConnectionHolder {

    private final AmqpClient client;
    private final AmqpConnectorCommonConfiguration configuration;
    private final AtomicReference<CurrentConnection> holder = new AtomicReference<>();

    private final Vertx vertx;
    private Consumer<Throwable> callback;

    public ConnectionHolder(AmqpClient client,
            AmqpConnectorCommonConfiguration configuration,
            Vertx vertx) {
        this.client = client;
        this.configuration = configuration;
        this.vertx = vertx;
    }

    public Context getContext() {
        CurrentConnection connection = holder.get();
        if (connection != null) {
            return connection.context;
        } else {
            return null;
        }
    }

    public Vertx getVertx() {
        return vertx;
    }

    private static class CurrentConnection {
        final AmqpConnection connection;
        final Context context;

        private CurrentConnection(AmqpConnection connection, Context context) {
            this.connection = connection;
            this.context = context;
        }
    }

    public synchronized void onFailure(Consumer<Throwable> callback) {
        this.callback = callback;
    }

    public Uni<AmqpConnection> getOrEstablishConnection() {
        return Uni.createFrom().item(() -> {
            CurrentConnection connection = holder.get();
            if (connection != null && connection.connection != null && !connection.connection.isDisconnected()) {
                return connection.connection;
            } else {
                return null;
            }
        })
                .onItem().ifNull().switchTo(() -> {
                    // we don't have a connection, try to connect.
                    Integer retryInterval = configuration.getReconnectInterval();
                    Integer retryAttempts = configuration.getReconnectAttempts();

                    CurrentConnection reference = holder.get();

                    if (reference != null && reference.connection != null && !reference.connection.isDisconnected()) {
                        AmqpConnection connection = reference.connection;
                        return Uni.createFrom().item(connection);
                    }

                    return client.connect()
                            .onSubscribe().invoke(s -> log.establishingConnection())
                            .onItem().transform(conn -> {
                                log.connectionEstablished();
                                holder.set(new CurrentConnection(conn, Vertx.currentContext()));
                                conn
                                        .exceptionHandler(t -> {
                                            holder.set(null);
                                            log.connectionFailure(t);

                                            // The callback failure allows propagating the failure downstream,
                                            // as we are disconnected from the flow.
                                            Consumer<Throwable> c;
                                            synchronized (this) {
                                                c = callback;
                                            }
                                            if (c != null) {
                                                c.accept(t);
                                            }
                                        });
                                // handle the case we are already disconnected.
                                if (conn.isDisconnected() || holder.get() == null) {
                                    // Throwing the exception would trigger a retry.
                                    holder.set(null);
                                    throw ex.illegalStateConnectionDisconnected();
                                }
                                return conn;
                            })
                            .onFailure()
                            .invoke(log::unableToConnectToBroker)
                            .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(retryInterval)).atMost(retryAttempts)
                            .onFailure().invoke(t -> {
                                holder.set(null);
                                log.unableToRecoverFromConnectionDisruption(t);
                            });
                });
    }

    public static CompletionStage<Void> runOnContext(Context context, Runnable runnable) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (Vertx.currentContext() == context) {
            runnable.run();
            future.complete(null);
        } else {
            context.runOnContext(x -> {
                runnable.run();
                future.complete(null);
            });
        }
        return future;
    }

    public static CompletionStage<Void> runOnContextAndReportFailure(Context context, Throwable reason, Runnable runnable) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (Vertx.currentContext() == context) {
            runnable.run();
            future.completeExceptionally(reason);
        } else {
            context.runOnContext(x -> {
                runnable.run();
                future.completeExceptionally(reason);
            });
        }
        return future;
    }
}
