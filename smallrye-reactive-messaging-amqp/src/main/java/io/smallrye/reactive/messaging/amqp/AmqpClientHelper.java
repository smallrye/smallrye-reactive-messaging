package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static io.vertx.core.net.ClientOptionsBase.DEFAULT_METRICS_NAME;

import java.util.Optional;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.providers.helpers.ConfigUtils;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.core.Vertx;

public class AmqpClientHelper {

    private AmqpClientHelper() {
        // avoid direct instantiation.
    }

    static AmqpClient createClient(AmqpConnector connector, AmqpConnectorCommonConfiguration config,
            Instance<AmqpClientOptions> amqpClientOptions,
            Instance<ClientCustomizer<AmqpClientOptions>> configCustomizers) {
        Optional<String> clientOptionsName = config.getClientOptionsName();
        AmqpClientOptions options;
        if (clientOptionsName.isPresent()) {
            options = createClientFromClientOptionsBean(amqpClientOptions, clientOptionsName.get(), config);
        } else {
            options = getOptionsFromChannel(config);
        }
        AmqpClientOptions clientOptions = ConfigUtils.customize(config.config(), configCustomizers, options);
        AmqpClient client = AmqpClient.create(connector.getVertx(), clientOptions);
        connector.addClient(client);
        return client;
    }

    static AmqpClient createClient(Vertx vertx, AmqpConnectorCommonConfiguration config,
            Instance<AmqpClientOptions> amqpClientOptions,
            Instance<ClientCustomizer<AmqpClientOptions>> configCustomizers) {
        Optional<String> clientOptionsName = config.getClientOptionsName();
        AmqpClientOptions options;
        if (clientOptionsName.isPresent()) {
            options = createClientFromClientOptionsBean(amqpClientOptions, clientOptionsName.get(), config);
        } else {
            options = getOptionsFromChannel(config);
        }
        AmqpClientOptions clientOptions = ConfigUtils.customize(config.config(), configCustomizers, options);
        return AmqpClient.create(vertx, clientOptions);
    }

    static AmqpClientOptions createClientFromClientOptionsBean(Instance<AmqpClientOptions> instance,
            String optionsBeanName, AmqpConnectorCommonConfiguration config) {
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

        // We must merge the channel config and the AMQP Client options.
        // In case of conflict, use the channel config.
        AmqpClientOptions customizerOptions = options.get();
        merge(customizerOptions, config);
        return customizerOptions;
    }

    /**
     * Checks whether the given attribute is set in the channel configuration.
     * It does not check for aliases or into the global connector configuration.
     *
     * @param attribute the attribute
     * @param configuration the configuration object
     * @return {@code true} is the given attribute is configured in the channel configuration
     */
    static boolean isSetInChannelConfiguration(String attribute, AmqpConnectorCommonConfiguration configuration) {
        return configuration.config().getConfigValue(attribute).getRawValue() != null;
    }

    /**
     * Merges the values from {@code channel} (the channel configuration), into the {@code custom}.
     * Values from {@code channel} replaces the values from {@code custom}.
     *
     * @param custom the custom configuration
     * @param config the common configuration
     */
    static void merge(AmqpClientOptions custom, AmqpConnectorCommonConfiguration config) {
        AmqpClientOptions channel = getOptionsFromChannel(config);
        String hostFromChannel = channel.getHost();
        int portFromChannel = channel.getPort();

        if (isSetInChannelConfiguration("username", config)) {
            custom.setUsername(channel.getUsername());
        }

        // If the username is not set, use the one from the channel (alias or connector config)
        if (custom.getUsername() == null) {
            custom.setUsername(channel.getUsername());
        }

        if (isSetInChannelConfiguration("password", config)) {
            custom.setPassword(channel.getPassword());
        }

        // If the password is not set, use the one from the channel (alias or connector config)
        if (custom.getPassword() == null) {
            custom.setPassword(channel.getPassword());
        }

        if (isSetInChannelConfiguration("host", config)) {
            custom.setHost(hostFromChannel);
        }

        // If the host is not set, use the one from the channel (alias or connector config)
        if (custom.getHost() == null) {
            custom.setHost(channel.getHost());
        }

        if (isSetInChannelConfiguration("port", config)) {
            custom.setPort(portFromChannel);
        }

        // custom.getPort() will return a value (might be the default port, or not, or the default port as set by the user)
        // the channel port may be set (using the alias or connector config)
        // we can compare, but we cannot decide which one is going to be used.
        // so, we use the one from the customizer except is explicitly set on the channel.
        // The same apply for ssl, reconnect-attempts, reconnect-interval and connect-timeout.

        if (isSetInChannelConfiguration("use-ssl", config)) {
            custom.setSsl(channel.isSsl());
        }

        if (isSetInChannelConfiguration("reconnect-attempts", config)) {
            custom.setReconnectAttempts(channel.getReconnectAttempts());
        }
        if (isSetInChannelConfiguration("reconnect-interval", config)) {
            custom.setReconnectInterval(channel.getReconnectInterval());
        }
        if (isSetInChannelConfiguration("connect-timeout", config)) {
            custom.setConnectTimeout(channel.getConnectTimeout());
        }

        if (DEFAULT_METRICS_NAME.equals(custom.getMetricsName())) {
            custom.setMetricsName(channel.getMetricsName());
        }
    }

    static AmqpClientOptions getOptionsFromChannel(AmqpConnectorCommonConfiguration config) {
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

        options.setMetricsName("amqp|" + config.getChannel());

        config.getSniServerName().ifPresent(options::setSniServerName);
        config.getVirtualHost().ifPresent(options::setVirtualHost);
        return options;
    }

}
