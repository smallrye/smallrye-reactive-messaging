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
    private final AtomicReference<Context> rootContext;
    private final AtomicReference<CompletionStage<RabbitMQClient>> connectionStage = new AtomicReference<>();

    private final Vertx vertx;
    private final RabbitMQConnectorCommonConfiguration configuration;

    public ClientHolder(RabbitMQClient client,
            RabbitMQConnectorCommonConfiguration configuration,
            Vertx vertx,
            Context root) {
        this.client = client;
        this.configuration = configuration;
        this.vertx = vertx;
        this.rootContext = new AtomicReference<>(root);
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

    public void ensureContext(Context context) {
        if (context == null) {
            return;
        }
        rootContext.compareAndSet(null, context);
        CurrentConnection connection = connectionHolder.get();
        if (connection != null && connection.context == null) {
            connectionHolder.compareAndSet(connection, new CurrentConnection(connection.client, context));
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
        CompletionStage<RabbitMQClient> existing = connectionStage.get();
        if (existing != null) {
            if (!existing.toCompletableFuture().isDone() || client.isConnected()) {
                return Uni.createFrom().completionStage(existing);
            }
            connectionStage.compareAndSet(existing, null);
        }

        for (;;) {
            CompletionStage<RabbitMQClient> current = connectionStage.get();
            if (current != null) {
                return Uni.createFrom().completionStage(current);
            }
            CompletionStage<RabbitMQClient> created = createConnectionUni().subscribeAsCompletionStage();
            if (connectionStage.compareAndSet(null, created)) {
                created.whenComplete((result, error) -> {
                    if (error != null) {
                        connectionStage.compareAndSet(created, null);
                    }
                });
                return Uni.createFrom().completionStage(created);
            }
        }
    }

    private static class CurrentConnection {

        final RabbitMQClient client;
        final Context context;

        private CurrentConnection(RabbitMQClient client, Context context) {
            this.client = client;
            this.context = context;
        }
    }

    private Uni<RabbitMQClient> createConnectionUni() {
        return Uni.createFrom().deferred(() -> client.start()
                .onSubscription().invoke(() -> {
                    connected.set(true);
                    log.connectionEstablished(configuration.getChannel());
                })
                .onItem().transform(ignored -> {
                    Context context = rootContext.get();
                    if (context == null) {
                        context = Vertx.currentContext();
                    }
                    connectionHolder.set(new CurrentConnection(client, context));

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
                }));
    }

}
