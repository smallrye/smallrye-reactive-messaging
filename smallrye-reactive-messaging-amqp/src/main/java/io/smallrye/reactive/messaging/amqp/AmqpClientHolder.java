package io.smallrye.reactive.messaging.amqp;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class AmqpClientHolder {

    private final AmqpClient client;
    private final Set<String> channels = ConcurrentHashMap.newKeySet();
    private volatile ConnectionHolder connectionHolder;

    public AmqpClientHolder(AmqpClient client) {
        this.client = client;
    }

    public AmqpClient client() {
        return client;
    }

    public AmqpClientHolder retain(String channel) {
        channels.add(channel);
        return this;
    }

    public synchronized ConnectionHolder getOrCreateConnectionHolder(
            AmqpConnectorCommonConfiguration config,
            Vertx vertx,
            Context rootContext) {
        if (connectionHolder == null) {
            connectionHolder = new ConnectionHolder(client, config, vertx, rootContext);
        }
        return connectionHolder;
    }
}
