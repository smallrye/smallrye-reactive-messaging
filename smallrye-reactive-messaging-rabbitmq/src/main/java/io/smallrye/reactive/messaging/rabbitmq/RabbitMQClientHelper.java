package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.Optional;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.i18n.ProviderLogging;
import io.vertx.core.net.JksOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;

public class RabbitMQClientHelper {

    private RabbitMQClientHelper() {
        // avoid direct instantiation.
    }

    static RabbitMQClient createClient(RabbitMQConnector connector, RabbitMQConnectorCommonConfiguration config,
            Instance<RabbitMQOptions> instance) {
        RabbitMQClient client;
        Optional<String> clientOptionsName = config.getClientOptionsName();
        Vertx vertx = connector.getVertx();
        if (clientOptionsName.isPresent()) {
            client = createClientFromClientOptionsBean(vertx, instance, clientOptionsName.get());
        } else {
            client = getClient(vertx, config);
        }
        connector.addClient(client);
        return client;
    }

    static RabbitMQClient createClientFromClientOptionsBean(Vertx vertx, Instance<RabbitMQOptions> instance,
            String optionsBeanName) {
        Instance<RabbitMQOptions> options = instance.select(Identifier.Literal.of(optionsBeanName));
        if (options.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            options = instance.select(NamedLiteral.of(optionsBeanName));
            if (!options.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }
        if (options.isUnsatisfied()) {
            throw ex.illegalStateFindingBean(RabbitMQOptions.class.getName(), optionsBeanName);
        }
        log.createClientFromBean(optionsBeanName);
        return RabbitMQClient.create(vertx, options.get());
    }

    static RabbitMQClient getClient(Vertx vertx, RabbitMQConnectorCommonConfiguration config) {
        try {
            String username = config.getUsername().orElse(RabbitMQOptions.DEFAULT_USER);
            String password = config.getPassword().orElse(RabbitMQOptions.DEFAULT_PASSWORD);
            String host = config.getHost();
            int port = config.getPort();
            log.brokerConfigured(host, port, config.getChannel());

            RabbitMQOptions options = new RabbitMQOptions()
                    .setUser(username)
                    .setPassword(password)
                    .setHost(host)
                    .setPort(port)
                    .setSsl(config.getSsl())
                    .setTrustAll(config.getTrustAll())
                    .setAutomaticRecoveryEnabled(config.getAutomaticRecoveryEnabled())
                    .setAutomaticRecoveryOnInitialConnection(config.getAutomaticRecoveryOnInitialConnection())
                    .setReconnectAttempts(config.getReconnectAttempts())
                    .setReconnectInterval(config.getReconnectInterval())
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

            return RabbitMQClient.create(vertx, options);
        } catch (Exception e) {
            log.unableToCreateClient(e);
            throw ex.illegalStateUnableToCreateClient(e);
        }
    }
}
