package io.smallrye.reactive.messaging.rabbitmq.internals;

import static com.rabbitmq.client.impl.DefaultCredentialsRefreshService.fixedTimeApproachingExpirationStrategy;
import static com.rabbitmq.client.impl.DefaultCredentialsRefreshService.ratioRefreshDelayStrategy;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;
import static io.vertx.core.net.ClientOptionsBase.DEFAULT_METRICS_NAME;
import static java.time.Duration.ofSeconds;

import java.time.Duration;
import java.util.*;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.DefaultCredentialsRefreshService;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorIncomingConfiguration;
import io.vertx.core.json.JsonObject;
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
        Optional<String> clientOptionsName = config.getClientOptionsName();
        Vertx vertx = connector.vertx();
        RabbitMQOptions options;
        try {
            if (clientOptionsName.isPresent()) {
                options = getClientOptionsFromBean(optionsInstances, clientOptionsName.get());
            } else {
                options = getClientOptions(vertx, config, credentialsProviderInstances);
            }
            if (DEFAULT_METRICS_NAME.equals(options.getMetricsName())) {
                options.setMetricsName("rabbitmq|" + config.getChannel());
            }
            RabbitMQClient client = RabbitMQClient.create(vertx, options);
            connector.addClient(config.getChannel(), client);
            return client;
        } catch (Exception e) {
            log.unableToCreateClient(e);
            throw ex.illegalStateUnableToCreateClient(e);
        }
    }

    static RabbitMQOptions getClientOptionsFromBean(Instance<RabbitMQOptions> options, String optionsBeanName) {
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
        return options.get();
    }

    static RabbitMQOptions getClientOptions(Vertx vertx, RabbitMQConnectorCommonConfiguration config,
            Instance<CredentialsProvider> credentialsProviders) {
        String connectionName = String.format("%s (%s)",
                config.getChannel(),
                config instanceof RabbitMQConnectorIncomingConfiguration ? "Incoming" : "Outgoing");
        List<Address> addresses = config.getAddresses()
                .map(s -> Arrays.asList(Address.parseAddresses(s)))
                .orElseGet(() -> Collections.singletonList(new Address(config.getHost(), config.getPort())));
        log.brokerConfigured(addresses.toString(), config.getChannel());

        RabbitMQOptions options = new RabbitMQOptions()
                .setConnectionName(connectionName)
                .setAddresses(addresses)
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

        return options;
    }

    public static String getExchangeName(final RabbitMQConnectorCommonConfiguration config) {
        return config.getExchangeName().map(s -> "\"\"".equals(s) ? "" : s).orElse(config.getChannel());
    }

    public static String serverQueueName(String name) {
        if (name.equals("(server.auto)")) {
            return "";
        }
        return name;
    }

    public static Map<String, Object> parseArguments(
            final Optional<String> argumentsConfig) {
        Map<String, Object> argumentsBinding = new HashMap<>();
        if (argumentsConfig.isPresent()) {
            for (String segment : argumentsConfig.get().split(",")) {
                String[] argumentKeyValueSplit = segment.trim().split(":");
                if (argumentKeyValueSplit.length == 2) {
                    String key = argumentKeyValueSplit[0];
                    String value = argumentKeyValueSplit[1];
                    argumentsBinding.put(key, value);
                }
            }
        }
        return argumentsBinding;
    }

    /**
     * Uses a {@link RabbitMQClient} to ensure the required exchange is created.
     *
     * @param client the RabbitMQ client
     * @param config the channel configuration
     * @return a {@link Uni <String>} which yields the exchange name
     */
    public static Uni<String> declareExchangeIfNeeded(
            final RabbitMQClient client,
            final RabbitMQConnectorCommonConfiguration config,
            final Instance<Map<String, ?>> configMaps) {
        final String exchangeName = getExchangeName(config);

        JsonObject queueArgs = new JsonObject();
        Instance<Map<String, ?>> queueArguments = CDIUtils.getInstanceById(configMaps, config.getExchangeArguments());
        if (queueArguments.isResolvable()) {
            Map<String, ?> argsMap = queueArguments.get();
            argsMap.forEach(queueArgs::put);
        }

        // Declare the exchange if we have been asked to do so and only when exchange name is not default ("")
        boolean declareExchange = config.getExchangeDeclare() && !exchangeName.isEmpty();
        if (declareExchange) {
            return client
                    .exchangeDeclare(exchangeName, config.getExchangeType(),
                            config.getExchangeDurable(), config.getExchangeAutoDelete(), queueArgs)
                    .replaceWith(exchangeName)
                    .invoke(() -> log.exchangeEstablished(exchangeName))
                    .onFailure().invoke(ex -> log.unableToEstablishExchange(exchangeName, ex));
        } else {
            return Uni.createFrom().item(exchangeName);
        }
    }

}
