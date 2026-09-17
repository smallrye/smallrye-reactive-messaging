package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;

public class ClientHolder {

    private final RabbitMQClient client;

    private final AtomicBoolean hasBeenConnected = new AtomicBoolean(false);
    private final Uni<RabbitMQClient> connection;
    private final Set<String> channels = ConcurrentHashMap.newKeySet();

    public ClientHolder(RabbitMQClient client) {
        this.client = client;
        this.connection = Uni.createFrom().deferred(() -> client.start()
                .onSubscription().invoke(() -> {
                    hasBeenConnected.set(true);
                    log.connectionEstablished(String.join(", ", channels));
                })
                .onItem().transform(ignored -> {
                    // handle the case we are already disconnected.
                    if (!client.isConnected()) {
                        // Throwing the exception would trigger a retry.
                        throw ex.illegalStateConnectionDisconnected();
                    }
                    return client;
                })
                .onFailure().invoke(log::unableToConnectToBroker))
                .memoize().until(() -> !client.isConnected());
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

    public RabbitMQClient client() {
        return client;
    }

    public boolean hasBeenConnected() {
        return hasBeenConnected.get();
    }

    @CheckReturnValue
    public Uni<Void> getAck(final long deliveryTag) {
        return client.basicAck(deliveryTag, false);
    }

    public Function<Throwable, Uni<Void>> getNack(final long deliveryTag, final boolean requeue) {
        return t -> client.basicNack(deliveryTag, false, requeue);
    }

    @CheckReturnValue
    public Uni<RabbitMQClient> getOrEstablishConnection() {
        return connection;
    }

    public Set<String> channels() {
        return channels;
    }

    public ClientHolder retain(String channel) {
        channels.add(channel);
        return this;
    }

    public boolean release(String channel) {
        channels.remove(channel);
        return channels.isEmpty();
    }

}
