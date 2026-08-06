package io.smallrye.reactive.messaging.amqp;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.smallrye.reactive.messaging.amqp.cbs.CbsTokenManager;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class AmqpClientHolder {

    private final AmqpClient client;
    private final Set<String> channels = ConcurrentHashMap.newKeySet();
    private ConnectionHolder connectionHolder;

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

    public boolean release(String channel) {
        channels.remove(channel);
        if (channels.isEmpty()) {
            closeConnectionHolder();
            return true;
        }
        return false;
    }

    void closeConnectionHolder() {
        if (connectionHolder != null) {
            connectionHolder.close();
        }
    }

    /**
     * Called during channel initialization which happens sequentially on a single thread.
     */
    public ConnectionHolder getOrCreateConnectionHolder(
            CbsTokenManager cbsTokenManager,
            AmqpConnectorCommonConfiguration config,
            Vertx vertx,
            Context rootContext) {
        if (connectionHolder == null) {
            connectionHolder = new ConnectionHolder(client, cbsTokenManager, config, vertx, rootContext);
        }
        return connectionHolder;
    }
}
