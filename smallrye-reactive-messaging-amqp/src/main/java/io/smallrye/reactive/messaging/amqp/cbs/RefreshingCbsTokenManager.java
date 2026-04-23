package io.smallrye.reactive.messaging.amqp.cbs;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static java.time.temporal.ChronoUnit.MILLIS;

import java.time.OffsetDateTime;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class RefreshingCbsTokenManager implements CbsTokenManager {

    private static final long MIN_REFRESH_DELAY_MS = 1000;

    private final CbsTokenManager tokenManager;
    private final Vertx vertx;
    private final Context root;
    private final Runnable onExpiration;
    private volatile long timerId;
    private volatile boolean authorized;

    public RefreshingCbsTokenManager(CbsTokenManager tokenManager, Vertx vertx, Context root, Runnable onExpiration) {
        this.tokenManager = tokenManager;
        this.vertx = vertx;
        this.root = root;
        this.onExpiration = onExpiration;
    }

    @Override
    public CbsExchange exchange(AmqpConnection connection) {
        return tokenManager.exchange(connection);
    }

    @Override
    public Uni<CbsToken> authorize(CbsExchange exchange) {
        if (timerId != 0) {
            vertx.cancelTimer(timerId);
        }
        return authorizeWithConnection(exchange);
    }

    Uni<CbsToken> authorizeWithConnection(CbsExchange exchange) {
        return tokenManager.authorize(exchange)
                .invoke(t -> refreshToken(exchange,
                        Math.max(MILLIS.between(OffsetDateTime.now(), t.refreshAt()), MIN_REFRESH_DELAY_MS)));
    }

    private void refreshToken(CbsExchange exchange, long delay) {
        authorized = true;
        log.infof("Token refreshed, next refresh in %s ms", delay);
        timerId = vertx.setTimer(delay,
                x -> VertxContext.runOnContext(root.getDelegate(), () -> authorizeWithConnection(exchange)
                        .subscribe().with(ignored -> {
                        }, failure -> {
                            authorized = false;
                            log.errorf(failure, "Token refresh failed, triggering reconnection");
                            onExpiration.run();
                        })));
    }

    @Override
    public boolean isAuthorized() {
        return authorized;
    }

    @Override
    public void close() {
        vertx.cancelTimer(timerId);
        tokenManager.close();
    }
}
