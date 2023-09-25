package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;

public class ConnectionHolder {

    private final RabbitMQClient client;
    private final AtomicReference<CurrentConnection> connectionHolder = new AtomicReference<>();
    private final Uni<RabbitMQClient> connector;

    private final Vertx vertx;

    public ConnectionHolder(RabbitMQClient client,
            RabbitMQConnectorCommonConfiguration configuration,
            Vertx vertx) {
        this.client = client;
        this.vertx = vertx;
        this.connector = Uni.createFrom().voidItem()
                .onItem().transformToUni(unused -> {
                    log.establishingConnection(configuration.getChannel());
                    return client.start()
                            .onSubscription().invoke(() -> log.connectionEstablished(configuration.getChannel()))
                            .onItem().transform(ignored -> {
                                connectionHolder.set(new CurrentConnection(client, Vertx.currentContext()));

                                // handle the case we are already disconnected.
                                if (!client.isConnected() || connectionHolder.get() == null) {
                                    // Throwing the exception would trigger a retry.
                                    connectionHolder.set(null);
                                    throw ex.illegalStateConnectionDisconnected();
                                }

                                return client;
                            })
                            .onFailure().invoke(log::unableToConnectToBroker)
                            .onFailure().invoke(t -> {
                                connectionHolder.set(null);
                                log.unableToRecoverFromConnectionDisruption(t);
                            });
                })
                .memoize().until(() -> {
                    CurrentConnection connection = connectionHolder.get();
                    if (connection == null) {
                        return true;
                    }
                    return !connection.connection.isConnected();
                });

    }

    public static CompletionStage<Void> runOnContext(Context context, IncomingRabbitMQMessage<?> msg,
            Consumer<IncomingRabbitMQMessage<?>> handle) {
        return VertxContext.runOnContext(context.getDelegate(), f -> {
            handle.accept(msg);
            msg.runOnMessageContext(() -> f.complete(null));
        });
    }

    public static CompletionStage<Void> runOnContextAndReportFailure(Context context,
            Throwable reason, IncomingRabbitMQMessage<?> msg, Consumer<IncomingRabbitMQMessage<?>> handle) {
        return VertxContext.runOnContext(context.getDelegate(), f -> {
            handle.accept(msg);
            msg.runOnMessageContext(() -> f.completeExceptionally(reason));
        });
    }

    public Context getContext() {
        CurrentConnection connection = connectionHolder.get();
        if (connection != null) {
            return connection.context;
        } else {
            return null;
        }
    }

    @CheckReturnValue
    public Uni<Void> getAck(final long deliveryTag) {
        return client.basicAck(deliveryTag, false);
    }

    public Function<Throwable, Uni<Void>> getNack(final long deliveryTag, final boolean requeue) {
        return t -> client.basicNack(deliveryTag, false, requeue);
    }

    public Vertx getVertx() {
        return vertx;
    }

    @CheckReturnValue
    public Uni<RabbitMQClient> getOrEstablishConnection() {
        return connector;
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
