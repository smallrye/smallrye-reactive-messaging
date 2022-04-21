package io.smallrye.reactive.messaging.rabbitmq;

import static com.rabbitmq.client.impl.DefaultCredentialsRefreshService.fixedTimeApproachingExpirationStrategy;
import static com.rabbitmq.client.impl.DefaultCredentialsRefreshService.ratioRefreshDelayStrategy;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;
import static java.time.Duration.ofSeconds;

import java.time.Duration;
import java.util.Optional;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.DefaultCredentialsRefreshService;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import io.vertx.core.net.JksOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;

public class RabbitMQClientHelper {

    private static final double CREDENTIALS_PROVIDER_REFRESH_DELAY_RATIO = 0.8;
    private static final Duration CREDENTIALS_PROVIDER_APPROACH_EXPIRE_TIME = ofSeconds(1);

    private RabbitMQClientHelper() {
        // avoid direct instantiation.
    }

    static RabbitMQClient createClient(RabbitMQConnector connector, RabbitMQConnectorCommonConfiguration config,
            Instance<RabbitMQOptions> optionsInstances, Instance<CredentialsProvider> credentialsProviderInstances) {
        RabbitMQClient client;
        Optional<String> clientOptionsName = config.getClientOptionsName();
        Vertx vertx = connector.getVertx();
        if (clientOptionsName.isPresent()) {
            client = createClientFromClientOptionsBean(vertx, optionsInstances, clientOptionsName.get());
        } else {
            client = getClient(vertx, config, credentialsProviderInstances);
        }
        connector.addClient(client);
        return client;
    }

    static RabbitMQClient createClientFromClientOptionsBean(Vertx vertx, Instance<RabbitMQOptions> options,
            String optionsBeanName) {
        options = options.select(Identifier.Literal.of(optionsBeanName));
        if (options.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            options = options.select(NamedLiteral.of(optionsBeanName));
            if (!options.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }
        if (!options.isResolvable()) {
            throw ex.illegalStateFindingBean(RabbitMQOptions.class.getName(), optionsBeanName);
        }
        log.createClientFromBean(optionsBeanName);
        return RabbitMQClient.create(vertx, options.get());
    }

    static RabbitMQClient getClient(Vertx vertx, RabbitMQConnectorCommonConfiguration config,
            Instance<CredentialsProvider> credentialsProviders) {
        try {
            String host = config.getHost();
            int port = config.getPort();
            log.brokerConfigured(host, port, config.getChannel());

            RabbitMQOptions options = new RabbitMQOptions()
                    .setHost(host)
                    .setPort(port)
                    .setSsl(config.getSsl())
                    .setTrustAll(config.getTrustAll())
                    .setAutomaticRecoveryEnabled(config.getAutomaticRecoveryEnabled())
                    .setAutomaticRecoveryOnInitialConnection(config.getAutomaticRecoveryOnInitialConnection())
                    .setReconnectAttempts(config.getReconnectAttempts())
                    .setReconnectInterval(ofSeconds(config.getReconnectInterval()).toMillis())
                    .setConnectionTimeout(config.getConnectionTimeout())
                    .setHandshakeTimeout(config.getHandshakeTimeout())
                    .setIncludeProperties(config.getIncludeProperties())
                    .setNetworkRecoveryInterval(config.getNetworkRecoveryInterval())
                    .setRequestedChannelMax(config.getRequestedChannelMax())
                    .setRequestedHeartbeat(config.getRequestedHeartbeat())
                    .setUseNio(config.getUseNio())
                    .setVirtualHost(config.getVirtualHost());

            // JKS TrustStore
            Optional<String> trustStorePath = config.getTrustStorePath();
            if (trustStorePath.isPresent()) {
                JksOptions jks = new JksOptions();
                jks.setPath(trustStorePath.get());
                config.getTrustStorePassword().ifPresent(jks::setPassword);
                options.setTrustStoreOptions(jks);
            }

            if (config.getCredentialsProviderName().isPresent()) {

                String credentialsProviderName = config.getCredentialsProviderName().get();
                credentialsProviders = credentialsProviders.select(Identifier.Literal.of(credentialsProviderName));
                if (credentialsProviders.isUnsatisfied()) {
                    // this `if` block should be removed when support for the `@Named` annotation is removed
                    credentialsProviders = credentialsProviders.select(NamedLiteral.of(credentialsProviderName));
                    if (!credentialsProviders.isUnsatisfied()) {
                        ProviderLogging.log.deprecatedNamed();
                    }
                }
                if (!credentialsProviders.isResolvable()) {
                    throw ex.illegalStateFindingBean(CredentialsProvider.class.getName(), credentialsProviderName);
                }

                CredentialsProvider credentialsProvider = credentialsProviders.get();
                options.setCredentialsProvider(credentialsProvider);

                // To ease configuration, set up a "standard" refresh service
                options.setCredentialsRefreshService(
                        new DefaultCredentialsRefreshService(
                                vertx.nettyEventLoopGroup(),
                                ratioRefreshDelayStrategy(CREDENTIALS_PROVIDER_REFRESH_DELAY_RATIO),
                                fixedTimeApproachingExpirationStrategy(CREDENTIALS_PROVIDER_APPROACH_EXPIRE_TIME)));
            } else {

                String username = config.getUsername().orElse(RabbitMQOptions.DEFAULT_USER);
                String password = config.getPassword().orElse(RabbitMQOptions.DEFAULT_PASSWORD);

                options.setUser(username);
                options.setPassword(password);
            }

            return RabbitMQClient.create(vertx, options);
        } catch (Exception e) {
            log.unableToCreateClient(e);
            throw ex.illegalStateUnableToCreateClient(e);
        }
    }
}
