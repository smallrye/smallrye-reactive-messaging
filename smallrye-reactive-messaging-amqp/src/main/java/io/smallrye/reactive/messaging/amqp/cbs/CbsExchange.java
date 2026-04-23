package io.smallrye.reactive.messaging.amqp.cbs;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;
import io.vertx.mutiny.amqp.AmqpConnection;

public interface CbsExchange {

    interface Factory {
        CbsExchange create(AmqpConnection amqpConnection, AmqpConnectorCommonConfiguration config);
    }

    Uni<CbsToken> authorize(Uni<CbsToken> authentication);

    void close();

}
