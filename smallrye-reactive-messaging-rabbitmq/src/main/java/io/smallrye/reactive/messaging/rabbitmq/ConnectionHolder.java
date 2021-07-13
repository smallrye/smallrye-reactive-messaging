package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;

public class ConnectionHolder {
    private final RabbitMQClient client;
    private final RabbitMQConnectorCommonConfiguration configuration;
    private final AtomicReference<CurrentConnection> holder = new AtomicReference<>();

    private final Vertx vertx;

    public ConnectionHolder(RabbitMQClient client,
            RabbitMQConnectorCommonConfiguration configuration,
            Vertx vertx) {
        this.client = client;
        this.configuration = configuration;
        this.vertx = vertx;
    }

    public static CompletionStage<Void> runOnContext(Context context, Runnable runnable) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (Vertx.currentContext() == context) {
            runnable.run();
            future.complete(null);
        } else {
            context.runOnContext(() -> {
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
            context.runOnContext(() -> {
                runnable.run();
                future.completeExceptionally(reason);
            });
        }
        return future;
    }

    public Context getContext() {
        CurrentConnection connection = holder.get();
        if (connection != null) {
            return connection.context;
        } else {
            return null;
        }
    }

    public Uni<Void> getAck(final long deliveryTag) {
        return client.basicAck(deliveryTag, false);
    }

    public Function<Throwable, CompletionStage<Void>> getNack(final long deliveryTag, final boolean requeue) {
        return t -> client.basicNack(deliveryTag, false, requeue).subscribeAsCompletionStage();
    }

    public Vertx getVertx() {
        return vertx;
    }

    @SuppressWarnings("unused")
    public synchronized void onFailure(Consumer<Throwable> callback) {
        // As RabbitMQClient doesn't have a failure callback mechanism, there isn't much we can do here
    }

    public Uni<RabbitMQClient> getOrEstablishConnection() {
        return Uni.createFrom().item(() -> {
            final CurrentConnection connection = holder.get();
            if (connection != null && connection.connection != null && connection.connection.isConnected()) {
                return connection.connection;
            } else {
                return null;
            }
        })
                .onItem().ifNull().switchTo(() -> {
                    // we don't have a connection, try to connect.
                    CurrentConnection reference = holder.get();

                    if (reference != null && reference.connection != null && reference.connection.isConnected()) {
                        RabbitMQClient connection = reference.connection;
                        return Uni.createFrom().item(connection);
                    }

                    log.establishingConnection(configuration.getChannel());
                    return client.start()
                            .onSubscribe().invoke(() -> log.connectionEstablished(configuration.getChannel()))
                            .onItem().transform(ignored -> {
                                holder.set(new CurrentConnection(client, Vertx.currentContext()));

                                // handle the case we are already disconnected.
                                if (!client.isConnected() || holder.get() == null) {
                                    // Throwing the exception would trigger a retry.
                                    holder.set(null);
                                    throw ex.illegalStateConnectionDisconnected();
                                }

                                return client;
                            })
                            .onFailure().invoke(ex -> log.unableToConnectToBroker(ex))
                            .onFailure().invoke(t -> {
                                holder.set(null);
                                log.unableToRecoverFromConnectionDisruption(t);
                            });
                });
    }

    private static class CurrentConnection {
        final RabbitMQClient connection;
        final Context context;

        private CurrentConnection(RabbitMQClient connection, Context context) {
            this.connection = connection;
            this.context = context;
        }
    }

}
