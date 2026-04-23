package io.smallrye.reactive.messaging.amqp.cbs;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.amqp.ConnectionHolder;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.vertx.mutiny.amqp.AmqpConnection;

public class DefaultCbsTokenManager implements CbsTokenManager {

    private final CbsTokenProvider tokenProvider;
    private final CbsRetryOptions cbsRetryOptions;
    private final Instance<CbsExchange.Factory> exchanges;
    private final AmqpConnectorCommonConfiguration config;

    @ApplicationScoped
    @Identifier("default-cbs-token-manager")
    static class Factory implements CbsTokenManager.Factory {

        @Inject
        @Any
        Instance<CbsTokenProvider> tokenProvider;

        @Inject
        @Any
        Instance<CbsExchange.Factory> exchanges;

        @Override
        public CbsTokenManager create(AmqpConnectorCommonConfiguration config) {
            return new DefaultCbsTokenManager(tokenProvider.get(), exchanges, config);
        }
    }

    public DefaultCbsTokenManager(CbsTokenProvider tokenProvider, Instance<CbsExchange.Factory> exchanges,
            AmqpConnectorCommonConfiguration config) {
        this.tokenProvider = tokenProvider;
        this.exchanges = exchanges;
        this.config = config;
        this.cbsRetryOptions = new CbsRetryOptions(config.getRetryOnFailAttempts(), config.getRetryOnFailInterval());
    }

    private static final String CBS_CAPABILITY = "AMQP_CBS_V1_0";

    @Override
    public CbsExchange exchange(AmqpConnection connection) {
        List<String> capabilities = ConnectionHolder.capabilities(connection);
        log.infof("Creating CBS exchange for connection %s with remote offered capabilities: %s",
                connection.getDelegate().unwrap().getRemoteContainer(), capabilities);
        if (!capabilities.contains(CBS_CAPABILITY)) {
            log.warnf("CBS authorization is enabled but the broker does not advertise the %s capability", CBS_CAPABILITY);
        }
        return CDIUtils.getInstanceById(exchanges, config.getCbsExchange()).get().create(connection, config);
    }

    @Override
    public Uni<CbsToken> authorize(CbsExchange exchange) {
        return exchange
                .authorize(tokenProvider.getToken(config))
                .onFailure().invoke(t -> log.errorf(t, "Authorization failed: " + t.getMessage()))
                .plug(cbsRetryOptions::apply);
    }

    @Override
    public boolean isAuthorized() {
        return true;
    }

    @Override
    public void close() {

    }
}
