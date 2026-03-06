package io.smallrye.reactive.messaging.rabbitmq.og.internals;

import static com.rabbitmq.client.impl.DefaultCredentialsRefreshService.fixedTimeApproachingExpirationStrategy;
import static com.rabbitmq.client.impl.DefaultCredentialsRefreshService.ratioRefreshDelayStrategy;
import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.og.i18n.RabbitMQLogging.log;
import static java.time.Duration.ofSeconds;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.*;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.DefaultCredentialsRefreshService;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.helpers.ConfigUtils;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.og.RabbitMQConnectorIncomingConfiguration;

public class RabbitMQClientHelper {

    private static final double CREDENTIALS_PROVIDER_REFRESH_DELAY_RATIO = 0.8;
    private static final Duration CREDENTIALS_PROVIDER_APPROACH_EXPIRE_TIME = ofSeconds(1);

    private RabbitMQClientHelper() {
        // avoid direct instantiation.
    }

    public static ConnectionFactory createConnectionFactory(
            RabbitMQConnectorCommonConfiguration config,
            Instance<ConnectionFactory> connectionFactories,
            Instance<CredentialsProvider> credentialsProviders,
            Instance<ClientCustomizer<ConnectionFactory>> configCustomizers) {

        Optional<String> clientOptionsName = config.getClientOptionsName();
        ConnectionFactory factory;

        try {
            if (clientOptionsName.isPresent()) {
                factory = getConnectionFactoryFromBean(connectionFactories, clientOptionsName.get());
            } else {
                factory = getConnectionFactory(config, credentialsProviders);
            }
            return ConfigUtils.customize(config.config(), configCustomizers, factory);
        } catch (Exception e) {
            log.unableToCreateClient(e);
            throw ex.illegalStateUnableToCreateClient(e);
        }
    }

    static ConnectionFactory getConnectionFactoryFromBean(Instance<ConnectionFactory> factories, String beanName) {
        Instance<ConnectionFactory> selected = factories.select(Identifier.Literal.of(beanName));
        if (selected.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            selected = factories.select(NamedLiteral.of(beanName));
            if (!selected.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }
        if (!selected.isResolvable()) {
            throw ex.illegalStateFindingBean(ConnectionFactory.class.getName(), beanName);
        }
        log.createClientFromBean(beanName);
        return selected.get();
    }

    static ConnectionFactory getConnectionFactory(
            RabbitMQConnectorCommonConfiguration config,
            Instance<CredentialsProvider> credentialsProviders) {

        String connectionName = String.format("%s (%s)",
                config.getChannel(),
                config instanceof RabbitMQConnectorIncomingConfiguration ? "Incoming" : "Outgoing");

        Address[] addresses = config.getAddresses()
                .map(Address::parseAddresses)
                .orElseGet(() -> new Address[] { new Address(config.getHost(), config.getPort()) });

        log.brokerConfigured(Arrays.toString(addresses), config.getChannel());

        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(config.getHost());
        factory.setPort(config.getPort());

        // Connection name
        factory.setConnectionTimeout(config.getConnectionTimeout());
        factory.setHandshakeTimeout(config.getHandshakeTimeout());
        factory.setRequestedChannelMax(config.getRequestedChannelMax());
        factory.setRequestedHeartbeat(config.getRequestedHeartbeat());
        factory.setVirtualHost(config.getVirtualHost());
        factory.setAutomaticRecoveryEnabled(config.getAutomaticRecoveryEnabled());
        factory.setNetworkRecoveryInterval(config.getNetworkRecoveryInterval());
        factory.setTopologyRecoveryEnabled(false); // We manage topology ourselves

        // NIO support
        if (config.getUseNio()) {
            factory.useNio();
        }

        // SSL configuration
        if (config.getSsl()) {
            try {
                if (config.getTrustAll()) {
                    factory.useSslProtocol();
                } else {
                    SSLContext sslContext = createSSLContext(config);
                    factory.useSslProtocol(sslContext);
                }

                // Hostname verification
                if (!"NONE".equals(config.getSslHostnameVerificationAlgorithm())) {
                    factory.enableHostnameVerification();
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to configure SSL", e);
            }
        }

        // Credentials
        if (config.getCredentialsProviderName().isPresent()) {
            String credentialsProviderName = config.getCredentialsProviderName().get();
            Instance<CredentialsProvider> selected = credentialsProviders
                    .select(Identifier.Literal.of(credentialsProviderName));
            if (selected.isUnsatisfied()) {
                selected = credentialsProviders.select(NamedLiteral.of(credentialsProviderName));
                if (!selected.isUnsatisfied()) {
                    ProviderLogging.log.deprecatedNamed();
                }
            }
            if (!selected.isResolvable()) {
                throw ex.illegalStateFindingBean(CredentialsProvider.class.getName(), credentialsProviderName);
            }

            CredentialsProvider credentialsProvider = selected.get();
            factory.setCredentialsProvider(credentialsProvider);

            // Set up refresh service
            factory.setCredentialsRefreshService(
                    new DefaultCredentialsRefreshService.DefaultCredentialsRefreshServiceBuilder()
                            .refreshDelayStrategy(ratioRefreshDelayStrategy(CREDENTIALS_PROVIDER_REFRESH_DELAY_RATIO))
                            .approachingExpirationStrategy(
                                    fixedTimeApproachingExpirationStrategy(CREDENTIALS_PROVIDER_APPROACH_EXPIRE_TIME))
                            .build());
        } else {
            String username = config.getUsername().orElse(ConnectionFactory.DEFAULT_USER);
            String password = config.getPassword().orElse(ConnectionFactory.DEFAULT_PASS);
            factory.setUsername(username);
            factory.setPassword(password);
        }

        return factory;
    }

    private static SSLContext createSSLContext(RabbitMQConnectorCommonConfiguration config)
            throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {

        Optional<String> trustStorePath = config.getTrustStorePath();
        if (trustStorePath.isPresent()) {
            KeyStore trustStore = KeyStore.getInstance("JKS");
            try (FileInputStream fis = new FileInputStream(trustStorePath.get())) {
                char[] trustStorePassword = config.getTrustStorePassword()
                        .map(String::toCharArray)
                        .orElse(null);
                trustStore.load(fis, trustStorePassword);
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        } else {
            return SSLContext.getDefault();
        }
    }

    public static String serverQueueName(String name) {
        if (name.equals("(server.auto)")) {
            return "";
        }
        return name;
    }

    public static Map<String, Object> parseArguments(final Optional<String> argumentsConfig) {
        Map<String, Object> argumentsBinding = new HashMap<>();
        if (argumentsConfig.isPresent()) {
            for (String segment : argumentsConfig.get().split(",")) {
                String[] argumentKeyValueSplit = segment.trim().split(":");
                if (argumentKeyValueSplit.length == 2) {
                    String key = argumentKeyValueSplit[0];
                    String value = argumentKeyValueSplit[1];
                    try {
                        argumentsBinding.put(key, Integer.parseInt(value));
                    } catch (NumberFormatException nfe) {
                        argumentsBinding.put(key, value);
                    }
                }
            }
        }
        return argumentsBinding;
    }

    /**
     * Declare exchange if needed
     */
    public static void declareExchangeIfNeeded(
            final Channel channel,
            final RabbitMQConnectorCommonConfiguration config,
            final Instance<Map<String, ?>> configMaps) throws IOException {

        final String exchangeName = getExchangeName(config);

        Map<String, Object> exchangeArgs = new HashMap<>();
        if (configMaps != null) {
            Instance<Map<String, ?>> exchangeArguments = CDIUtils.getInstanceById(configMaps, config.getExchangeArguments());
            if (exchangeArguments.isResolvable()) {
                Map<String, ?> argsMap = exchangeArguments.get();
                exchangeArgs.putAll(argsMap);
            }
        }

        // Declare the exchange if we have been asked to do so and only when exchange name is not default ("")
        boolean declareExchange = config.getExchangeDeclare() && !exchangeName.isEmpty();
        if (declareExchange) {
            try {
                channel.exchangeDeclare(
                        exchangeName,
                        config.getExchangeType(),
                        config.getExchangeDurable(),
                        config.getExchangeAutoDelete(),
                        exchangeArgs);
                log.exchangeEstablished(exchangeName);
            } catch (IOException ex) {
                log.unableToEstablishExchange(exchangeName, ex);
                throw ex;
            }
        }
    }

    public static String getExchangeName(final RabbitMQConnectorCommonConfiguration config) {
        return config.getExchangeName().map(s -> "\"\"".equals(s) ? "" : s).orElse(config.getChannel());
    }

    /**
     * Declare queue with all arguments
     */
    public static String declareQueueIfNeeded(
            final Channel channel,
            final RabbitMQConnectorIncomingConfiguration ic,
            final Instance<Map<String, ?>> configMaps) throws IOException {

        final String queueName = getQueueName(ic);

        if (!ic.getQueueDeclare()) {
            // Not declaring the queue, just validate it exists
            try {
                channel.queueDeclarePassive(queueName);
                return queueName;
            } catch (IOException e) {
                log.unableToEstablishQueue(queueName, e);
                throw e;
            }
        }

        // Build queue arguments
        final Map<String, Object> queueArgs = new HashMap<>();
        if (configMaps != null) {
            Instance<Map<String, ?>> queueArguments = CDIUtils.getInstanceById(configMaps, ic.getQueueArguments());
            if (queueArguments.isResolvable()) {
                Map<String, ?> argsMap = queueArguments.get();
                queueArgs.putAll(argsMap);
            }
        }

        if (ic.getAutoBindDlq()) {
            queueArgs.put("x-dead-letter-exchange", ic.getDeadLetterExchange());
            queueArgs.put("x-dead-letter-routing-key", ic.getDeadLetterRoutingKey().orElse(queueName));
        }

        ic.getQueueSingleActiveConsumer().ifPresent(sac -> queueArgs.put("x-single-active-consumer", sac));
        ic.getQueueXQueueType().ifPresent(queueType -> queueArgs.put("x-queue-type", queueType));
        ic.getQueueXQueueMode().ifPresent(queueMode -> queueArgs.put("x-queue-mode", queueMode));
        ic.getQueueTtl().ifPresent(queueTtl -> {
            if (queueTtl >= 0) {
                queueArgs.put("x-message-ttl", queueTtl);
            } else {
                throw ex.illegalArgumentInvalidQueueTtl();
            }
        });
        ic.getQueueXMaxPriority().ifPresent(maxPriority -> queueArgs.put("x-max-priority", maxPriority));
        ic.getQueueXDeliveryLimit().ifPresent(deliveryLimit -> queueArgs.put("x-delivery-limit", deliveryLimit));

        String serverQueueName = serverQueueName(queueName);

        try {
            String actualQueueName;
            if (serverQueueName.isEmpty()) {
                // Server-generated queue name - capture the actual name from the response
                com.rabbitmq.client.AMQP.Queue.DeclareOk response = channel.queueDeclare(serverQueueName, false, true, true,
                        null);
                actualQueueName = response.getQueue();
            } else {
                channel.queueDeclare(
                        serverQueueName,
                        ic.getQueueDurable(),
                        ic.getQueueExclusive(),
                        ic.getQueueAutoDelete(),
                        queueArgs);
                actualQueueName = serverQueueName;
            }
            log.queueEstablished(actualQueueName);
            return actualQueueName;
        } catch (IOException ex) {
            log.unableToEstablishQueue(queueName, ex);
            throw ex;
        }
    }

    /**
     * Establish bindings from queue to exchange
     */
    public static void establishBindings(
            final Channel channel,
            final RabbitMQConnectorIncomingConfiguration ic) throws IOException {

        final String exchangeName = getExchangeName(ic);
        final String queueName = getQueueName(ic);
        final List<String> routingKeys = Arrays.asList(ic.getRoutingKeys().split(","));
        final Map<String, Object> arguments = parseArguments(ic.getArguments());

        // Skip queue bindings if exchange name is default ("")
        if (exchangeName.isEmpty()) {
            return;
        }

        for (String routingKey : routingKeys) {
            try {
                channel.queueBind(serverQueueName(queueName), exchangeName, routingKey.trim(), arguments);
                log.bindingEstablished(queueName, exchangeName, routingKey.trim(), arguments.toString());
            } catch (IOException ex) {
                log.unableToEstablishBinding(queueName, exchangeName, ex);
                throw ex;
            }
        }
    }

    /**
     * Configure DLQ and DLX
     */
    public static void configureDLQorDLX(
            final Channel channel,
            final RabbitMQConnectorIncomingConfiguration ic,
            final Instance<Map<String, ?>> configMaps) throws IOException {

        if (!ic.getAutoBindDlq()) {
            return;
        }

        final String deadLetterQueueName = ic.getDeadLetterQueueName().orElse(String.format("%s.dlq", getQueueName(ic)));
        final String deadLetterExchangeName = ic.getDeadLetterExchange();
        final String deadLetterRoutingKey = ic.getDeadLetterRoutingKey().orElse(getQueueName(ic));

        // Declare DLX if needed
        if (ic.getDlxDeclare()) {
            final Map<String, Object> exchangeArgs = new HashMap<>();
            if (configMaps != null) {
                ic.getDeadLetterExchangeArguments().ifPresent(argsId -> {
                    Instance<Map<String, ?>> exchangeArguments = CDIUtils.getInstanceById(configMaps, argsId);
                    if (exchangeArguments.isResolvable()) {
                        exchangeArgs.putAll(exchangeArguments.get());
                    }
                });
            }

            try {
                channel.exchangeDeclare(deadLetterExchangeName, ic.getDeadLetterExchangeType(), true, false, exchangeArgs);
                log.dlxEstablished(deadLetterExchangeName);
            } catch (IOException ex) {
                log.unableToEstablishDlx(deadLetterExchangeName, ex);
                throw ex;
            }
        }

        // Declare DLQ
        final Map<String, Object> queueArgs = new HashMap<>();
        if (configMaps != null) {
            ic.getDeadLetterQueueArguments().ifPresent(argsId -> {
                Instance<Map<String, ?>> queueArguments = CDIUtils.getInstanceById(configMaps, argsId);
                if (queueArguments.isResolvable()) {
                    queueArgs.putAll(queueArguments.get());
                }
            });
        }

        ic.getDeadLetterDlx().ifPresent(deadLetterDlx -> queueArgs.put("x-dead-letter-exchange", deadLetterDlx));
        ic.getDeadLetterDlxRoutingKey().ifPresent(deadLetterDlx -> queueArgs.put("x-dead-letter-routing-key", deadLetterDlx));
        ic.getDeadLetterQueueType().ifPresent(queueType -> queueArgs.put("x-queue-type", queueType));
        ic.getDeadLetterQueueMode().ifPresent(queueMode -> queueArgs.put("x-queue-mode", queueMode));
        ic.getDeadLetterTtl().ifPresent(queueTtl -> {
            if (queueTtl >= 0) {
                queueArgs.put("x-message-ttl", queueTtl);
            } else {
                throw ex.illegalArgumentInvalidQueueTtl();
            }
        });

        try {
            channel.queueDeclare(deadLetterQueueName, true, false, false, queueArgs);
            log.queueEstablished(deadLetterQueueName);

            channel.queueBind(deadLetterQueueName, deadLetterExchangeName, deadLetterRoutingKey);
            log.deadLetterBindingEstablished(deadLetterQueueName, deadLetterExchangeName, deadLetterRoutingKey);
        } catch (IOException ex) {
            log.unableToEstablishQueue(deadLetterQueueName, ex);
            throw ex;
        }
    }

    public static String getQueueName(final RabbitMQConnectorIncomingConfiguration config) {
        return config.getQueueName().orElse(config.getChannel());
    }
}
