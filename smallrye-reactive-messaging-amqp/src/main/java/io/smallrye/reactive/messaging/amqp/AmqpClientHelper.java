package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

import java.util.Optional;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.i18n.ProviderLogging;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.core.Vertx;

public class AmqpClientHelper {

    private AmqpClientHelper() {
        // avoid direct instantiation.
    }

    static AmqpClient createClient(AmqpConnector connector, AmqpConnectorCommonConfiguration config,
            Instance<AmqpClientOptions> instance) {
        AmqpClient client;
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

    static AmqpClient createClientFromClientOptionsBean(Vertx vertx, Instance<AmqpClientOptions> instance,
            String optionsBeanName) {
        Instance<AmqpClientOptions> options = instance.select(Identifier.Literal.of(optionsBeanName));
        if (options.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            options = instance.select(NamedLiteral.of(optionsBeanName));
            if (!options.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }
        if (options.isUnsatisfied()) {
            throw ex.illegalStateFindingBean(AmqpClientOptions.class.getName(), optionsBeanName);
        }
        log.createClientFromBean(optionsBeanName);
        return AmqpClient.create(vertx, options.get());
    }

    static AmqpClient getClient(Vertx vertx, AmqpConnectorCommonConfiguration config) {
        try {
            String username = config.getUsername().orElse(null);
            String password = config.getPassword().orElse(null);
            String host = config.getHost();
            int port = config.getPort();
            log.brokerConfigured(host, port, config.getChannel());
            boolean useSsl = config.getUseSsl();
            int reconnectAttempts = config.getReconnectAttempts();
            int reconnectInterval = config.getReconnectInterval();
            int connectTimeout = config.getConnectTimeout();

            // We renamed containerID into container-id. So we must check both.
            String containerId = config.getContainerId()
                    .orElseGet(() -> config.config.getOptionalValue("containerId", String.class).orElse(null));

            AmqpClientOptions options = new AmqpClientOptions()
                    .setUsername(username)
                    .setPassword(password)
                    .setHost(host)
                    .setPort(port)
                    .setContainerId(containerId)
                    .setSsl(useSsl)
                    .setReconnectAttempts(reconnectAttempts)
                    .setReconnectInterval(reconnectInterval)
                    .setConnectTimeout(connectTimeout);

            config.getSniServerName().ifPresent(options::setSniServerName);
            config.getVirtualHost().ifPresent(options::setVirtualHost);

            return AmqpClient.create(vertx, options);
        } catch (Exception e) {
            log.unableToCreateClient(e);
            throw ex.illegalStateUnableToCreateClient(e);
        }
    }
}
