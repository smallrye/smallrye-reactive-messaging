package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;

public class ClientHolder {

    private final RabbitMQClient client;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicReference<CurrentConnection> connectionHolder = new AtomicReference<>();
    private final Uni<RabbitMQClient> connection;

    private final Vertx vertx;

    public ClientHolder(RabbitMQClient client,
            RabbitMQConnectorCommonConfiguration configuration,
            Vertx vertx,
            Context root) {
        this.client = client;
        this.vertx = vertx;
        this.connection = Uni.createFrom().deferred(() -> client.start()
                .onSubscription().invoke(() -> {
                    connected.set(true);
                    log.connectionEstablished(configuration.getChannel());
                })
                .onItem().transform(ignored -> {
                    connectionHolder
                            .set(new CurrentConnection(client, root == null ? Vertx.currentContext() : root));

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
                }))
                .memoize().until(() -> {
                    CurrentConnection connection = connectionHolder.get();
                    if (connection == null) {
                        return true;
                    }
                    return !connection.client.isConnected();
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

    public RabbitMQClient client() {
        return client;
    }

    public boolean hasBeenConnected() {
        return connected.get();
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
        return connection;
    }

    private static class CurrentConnection {

        final RabbitMQClient client;
        final Context context;

        private CurrentConnection(RabbitMQClient client, Context context) {
            this.client = client;
            this.context = context;
        }
    }

}
