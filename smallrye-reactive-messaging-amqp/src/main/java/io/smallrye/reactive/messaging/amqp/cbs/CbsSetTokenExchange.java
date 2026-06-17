package io.smallrye.reactive.messaging.amqp.cbs;

import static io.smallrye.reactive.messaging.amqp.cbs.CbsUtils.extractCbsNode;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpSender;

public class CbsSetTokenExchange implements CbsExchange {

    private final Uni<AmqpSender> senderUni;
    private volatile AmqpSender actualSender;

    @ApplicationScoped
    @Identifier("set-token")
    public static class Factory implements CbsExchange.Factory {

        @Override
        public CbsExchange create(AmqpConnection connection, AmqpConnectorCommonConfiguration config) {
            return new CbsSetTokenExchange(connection);
        }
    }

    public CbsSetTokenExchange(AmqpConnection connection) {
        this.senderUni = connection.createSender(extractCbsNode(connection))
                .onItem().invoke(s -> actualSender = s)
                .memoize().indefinitely();
    }

    @Override
    public Uni<CbsToken> authorize(Uni<CbsToken> authentication) {
        return senderUni.onItem().transformToUni(s -> authentication
                .flatMap(token -> s.sendWithAck(AmqpMessage.create()
                        .subject("set-token")
                        .applicationProperties(JsonObject.of("token-type", token.type()))
                        .withBody(token.token())
                        .build())
                        .replaceWith(token)));
    }

    @Override
    public void close() {
        if (actualSender != null) {
            actualSender.closeAndForget();
        }
    }
}
