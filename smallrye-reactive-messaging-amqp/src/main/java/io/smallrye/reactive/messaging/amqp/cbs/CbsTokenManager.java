package io.smallrye.reactive.messaging.amqp.cbs;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;
import io.vertx.mutiny.amqp.AmqpConnection;

public interface CbsTokenManager {

    interface Factory {

        CbsTokenManager create(AmqpConnectorCommonConfiguration config);
    }

    CbsExchange exchange(AmqpConnection connection);

    Uni<CbsToken> authorize(CbsExchange exchange);

    boolean isAuthorized();

    void close();
}
