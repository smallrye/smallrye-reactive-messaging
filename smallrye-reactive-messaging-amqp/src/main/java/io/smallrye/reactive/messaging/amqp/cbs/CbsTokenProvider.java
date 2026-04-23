package io.smallrye.reactive.messaging.amqp.cbs;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;

public interface CbsTokenProvider {

    Uni<CbsToken> getToken(AmqpConnectorCommonConfiguration config);

}
