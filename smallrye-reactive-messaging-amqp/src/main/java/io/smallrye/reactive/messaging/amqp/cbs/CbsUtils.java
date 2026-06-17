package io.smallrye.reactive.messaging.amqp.cbs;

import java.util.Optional;

import org.apache.qpid.proton.amqp.Symbol;

import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.proton.ProtonConnection;

public class CbsUtils {

    // TODO handle default node name in a more flexible way, possibly through configuration
    public static String extractCbsNode(AmqpConnection conn) {
        ProtonConnection protonConnection = conn.getDelegate().unwrap();
        return Optional.ofNullable(protonConnection.getRemoteProperties())
                .map(properties -> properties.get(Symbol.valueOf("cbs-node")))
                .map(Object::toString)
                .orElse("$cbs");
    }
}
