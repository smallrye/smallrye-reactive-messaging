package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static io.vertx.core.net.ClientOptionsBase.DEFAULT_METRICS_NAME;

import java.util.Optional;

import javax.net.ssl.SSLContext;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.spi.tls.SslContextFactory;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.core.Vertx;

public class AmqpClientHelper {

    private AmqpClientHelper() {
        // avoid direct instantiation.
    }

    static AmqpClient createClient(AmqpConnector connector, AmqpConnectorCommonConfiguration config,
            Instance<AmqpClientOptions> amqpClientOptions, Instance<SSLContext> clientSslContexts) {
        AmqpClient client;
        Optional<String> clientOptionsName = config.getClientOptionsName();
        Optional<String> clientSslContextName = config.getClientSslContextName();
        if (clientOptionsName.isPresent() && clientSslContextName.isPresent()) {
            throw ProviderLogging.log.cannotSpecifyBothClientOptionsNameAndClientSslContextName();
        }
        Vertx vertx = connector.getVertx();
        if (clientOptionsName.isPresent()) {
            client = createClientFromClientOptionsBean(vertx, amqpClientOptions, clientOptionsName.get(), config);
        } else {
            SSLContext sslContext = getClientSslContext(clientSslContexts, clientSslContextName);
            client = getClient(vertx, config, sslContext);
        }
        connector.addClient(client);
        return client;
    }

    static AmqpClient createClientFromClientOptionsBean(Vertx vertx, Instance<AmqpClientOptions> instance,
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
        return AmqpClient.create(vertx, customizerOptions);
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

    static AmqpClient getClient(Vertx vertx, AmqpConnectorCommonConfiguration config, SSLContext sslContext) {
        try {
            AmqpClientOptions options = getOptionsFromChannel(config);
            if (sslContext != null) {
                options.setSslEngineOptions(new JdkSSLEngineOptions() {
                    @Override
                    public SslContextFactory sslContextFactory() {
                        return new SslContextFactory() {
                            @Override
                            public SslContext create() {
                                return new JdkSslContext(
                                        sslContext,
                                        true,
                                        null,
                                        IdentityCipherSuiteFilter.INSTANCE,
                                        ApplicationProtocolConfig.DISABLED,
                                        io.netty.handler.ssl.ClientAuth.NONE,
                                        null,
                                        false);
                            }
                        };
                    }
                });
            }
            return AmqpClient.create(vertx, options);
        } catch (Exception e) {
            log.unableToCreateClient(e);
            throw ex.illegalStateUnableToCreateClient(e);
        }
    }

    private static SSLContext getClientSslContext(Instance<SSLContext> clientSslContexts,
            Optional<String> clientSslContextName) {
        if (clientSslContextName.isPresent()) {
            Instance<SSLContext> context = clientSslContexts
                    .select(Identifier.Literal.of(clientSslContextName.get()));
            if (context.isUnsatisfied()) {
                throw ProviderLogging.log.couldFindSslContextWithIdentifier(clientSslContextName.get());
            }
            return context.get();
        }
        return null;
    }
}
