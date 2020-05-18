package io.smallrye.reactive.messaging.amqp;

import java.util.Optional;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.amqp.AmqpClientOptions;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.core.Vertx;

public class AmqpClientHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpClientHelper.class);

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
        Instance<AmqpClientOptions> options = instance.select(NamedLiteral.of(optionsBeanName));
        if (options.isUnsatisfied()) {
            throw new IllegalStateException(
                    "Cannot find a " + AmqpClientOptions.class.getName() + " bean named " + optionsBeanName);
        }
        LOGGER.debug("Creating AMQP client from bean named '{}'", optionsBeanName);
        return AmqpClient.create(vertx, options.get());
    }

    static AmqpClient getClient(Vertx vertx, AmqpConnectorCommonConfiguration config) {
        try {
            String username = config.getUsername().orElse(null);
            String password = config.getPassword().orElse(null);
            String host = config.getHost();
            int port = config.getPort();
            LOGGER.info("AMQP broker configured to {}:{} for channel {}", host, port, config.getChannel());
            boolean useSsl = config.getUseSsl();
            int reconnectAttempts = config.getReconnectAttempts();
            int reconnectInterval = config.getReconnectInterval();
            int connectTimeout = config.getConnectTimeout();
            String containerId = config.getContainerId().orElse(null);

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
            return AmqpClient.create(vertx, options);
        } catch (Exception e) {
            LOGGER.error("Unable to create client", e);
            throw new IllegalStateException("Unable to create a client, probably a config error", e);
        }
    }
}
