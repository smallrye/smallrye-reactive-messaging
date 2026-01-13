package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;
import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.jms.fault.JmsFailureHandler;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;

@ApplicationScoped
@Connector(JmsConnector.CONNECTOR_NAME)

@ConnectorAttribute(name = "connection-factory-name", description = "The name of the JMS connection factory  (`jakarta.jms.ConnectionFactory`) to be used. If not set, it uses any exposed JMS connection factory", direction = Direction.INCOMING_AND_OUTGOING, type = "String")
@ConnectorAttribute(name = "username", description = "The username to connect to to the JMS server", direction = Direction.INCOMING_AND_OUTGOING, type = "String")
@ConnectorAttribute(name = "password", description = "The password to connect to to the JMS server", direction = Direction.INCOMING_AND_OUTGOING, type = "String")
@ConnectorAttribute(name = "session-mode", description = "The session mode. Accepted values are AUTO_ACKNOWLEDGE, SESSION_TRANSACTED, CLIENT_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE", direction = Direction.INCOMING_AND_OUTGOING, type = "String", defaultValue = "AUTO_ACKNOWLEDGE")
@ConnectorAttribute(name = "client-id", description = "The client id", direction = Direction.INCOMING_AND_OUTGOING, type = "String")
@ConnectorAttribute(name = "destination", description = "The name of the JMS destination. If not set the name of the channel is used", direction = Direction.INCOMING_AND_OUTGOING, type = "String")
@ConnectorAttribute(name = "selector", description = "The JMS selector", direction = Direction.INCOMING, type = "String")
@ConnectorAttribute(name = "no-local", description = "Enable or disable local delivery", direction = Direction.INCOMING, type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "broadcast", description = "Whether or not the JMS message should be dispatched to multiple consumers", direction = Direction.INCOMING, type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "durable", description = "Set to `true` to use a durable subscription", direction = Direction.INCOMING, type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "destination-type", description = "The type of destination. It can be either `queue` or `topic`", direction = Direction.INCOMING_AND_OUTGOING, type = "string", defaultValue = "queue")
@ConnectorAttribute(name = "tracing-enabled", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "reuse-jms-context", description = "Whether to reuse JMS contexts by creating child contexts from a parent context. When enabled, child contexts will be created using JMSContext.createContext() instead of ConnectionFactory.createContext()", direction = Direction.INCOMING_AND_OUTGOING, type = "boolean", defaultValue = "false")

@ConnectorAttribute(name = "disable-message-id", description = "Omit the message id in the outbound JMS message", direction = Direction.OUTGOING, type = "boolean")
@ConnectorAttribute(name = "disable-message-timestamp", description = "Omit the message timestamp in the outbound JMS message", direction = Direction.OUTGOING, type = "boolean")
@ConnectorAttribute(name = "delivery-mode", description = "The delivery mode. Either `persistent` or `non_persistent`", direction = Direction.OUTGOING, type = "string")
@ConnectorAttribute(name = "delivery-delay", description = "The delivery delay", direction = Direction.OUTGOING, type = "long")
@ConnectorAttribute(name = "ttl", description = "The JMS Message time-to-live", direction = Direction.OUTGOING, type = "long")
@ConnectorAttribute(name = "correlation-id", description = "The JMS Message correlation id", direction = Direction.OUTGOING, type = "string")
@ConnectorAttribute(name = "priority", description = "The JMS Message priority", direction = Direction.OUTGOING, type = "int")
@ConnectorAttribute(name = "reply-to", description = "The reply to destination if any", direction = Direction.OUTGOING, type = "string")
@ConnectorAttribute(name = "reply-to-destination-type", description = "The type of destination for the response. It can be either `queue` or `topic`", direction = Direction.OUTGOING, type = "string", defaultValue = "queue")
@ConnectorAttribute(name = "merge", direction = OUTGOING, description = "Whether the connector should allow multiple upstreams", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "retry", direction = Direction.INCOMING_AND_OUTGOING, description = "Whether to retry on terminal stream errors.", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "retry.max-retries", direction = Direction.INCOMING_AND_OUTGOING, description = "Maximum number of retries for terminal stream errors.", type = "int", defaultValue = "3")
@ConnectorAttribute(name = "retry.initial-delay", direction = Direction.INCOMING_AND_OUTGOING, description = "The initial delay for the retry.", type = "string", defaultValue = "PT1S")
@ConnectorAttribute(name = "retry.max-delay", direction = Direction.INCOMING_AND_OUTGOING, description = "The maximum delay", type = "string", defaultValue = "PT10S")
@ConnectorAttribute(name = "retry.jitter", direction = Direction.INCOMING_AND_OUTGOING, description = "How much the delay jitters as a multiplier between 0 and 1. The formula is current delay * jitter. For example, with a current delay of 2H, a jitter of 0.5 will result in an actual delay somewhere between 1H and 3H.", type = "double", defaultValue = "0.5")
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = Direction.INCOMING, description = "Specify the failure strategy to apply when a message produced from a record is acknowledged negatively (nack). Values can be `fail` (default), `ignore`, or `dead-letter-queue`", defaultValue = "fail")
@ConnectorAttribute(name = "dead-letter-queue.destination", type = "string", direction = Direction.INCOMING, description = "When the `failure-strategy` is set to `dead-letter-queue` indicates on which queue the message is sent. Defaults is `dead-letter-topic-$channel`")
@ConnectorAttribute(name = "dead-letter-queue.producer-client-id", type = "string", direction = Direction.INCOMING, description = "When the `failure-strategy` is set to `dead-letter-queue` indicates what client id the generated producer should use. Defaults is `jms-dead-letter-topic-producer-$client-id`")
public class JmsConnector implements InboundConnector, OutboundConnector {

    /**
     * The name of the connector: {@code smallrye-jms}
     */
    public static final String CONNECTOR_NAME = "smallrye-jms";

    /**
     * The default max-pool-size: {@code 10}
     */
    @SuppressWarnings("WeakerAccess")
    static final String DEFAULT_MAX_POOL_SIZE = "10";

    /**
     * The default thread ideal TTL: {@code 60} seconds
     */
    @SuppressWarnings("WeakerAccess")
    static final String DEFAULT_THREAD_TTL = "60";

    @Inject
    @Any
    Instance<ConnectionFactory> factories;

    @Inject
    Instance<JsonMapping> jsonMapper;

    @Inject
    @Any
    Instance<JmsFailureHandler.Factory> failureHandlerFactories;

    @Inject
    ExecutionHolder executionHolders;

    @Inject
    @ConfigProperty(name = "smallrye.jms.threads.max-pool-size", defaultValue = DEFAULT_MAX_POOL_SIZE)
    int maxPoolSize;

    @Inject
    @ConfigProperty(name = "smallrye.jms.threads.ttl", defaultValue = DEFAULT_THREAD_TTL)
    int ttl;

    @Inject
    Instance<OpenTelemetry> openTelemetryInstance;

    private ExecutorService executor;
    private JsonMapping jsonMapping;
    private final List<JmsSource> sources = new CopyOnWriteArrayList<>();
    private final List<JmsResourceHolder<?>> contexts = new CopyOnWriteArrayList<>();

    // Parent contexts for reuse, keyed by factory + credentials + session-mode combination
    private final List<ParentContextHolder> parentContexts = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void init() {
        this.executor = Executors.newFixedThreadPool(maxPoolSize);
        if (jsonMapper.isUnsatisfied()) {
            log.warn(
                    "Please add one of the additional mapping modules (-jsonb or -jackson) to be able to (de)serialize JSON messages.");
        } else if (jsonMapper.isAmbiguous()) {
            log.warn(
                    "Please select only one of the additional mapping modules (-jsonb or -jackson) to be able to (de)serialize JSON messages.");
            this.jsonMapping = jsonMapper.stream().findFirst()
                    .orElseThrow(() -> new RuntimeException("Unable to find JSON Mapper"));
        } else {
            this.jsonMapping = jsonMapper.get();
        }

    }

    @PreDestroy
    public void cleanup() {
        sources.forEach(JmsSource::close);
        contexts.forEach(JmsResourceHolder::close);
        // Clean up parent contexts
        parentContexts.forEach(ParentContextHolder::close);
        parentContexts.clear();
        this.executor.shutdown();
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        JmsConnectorIncomingConfiguration ic = new JmsConnectorIncomingConfiguration(config);
        JmsResourceHolder<JMSConsumer> holder = new JmsResourceHolder<>(ic.getChannel(), () -> createJmsContext(ic));
        contexts.add(holder);
        JmsSource source = new JmsSource(this, executionHolders.vertx(), holder, ic, openTelemetryInstance, jsonMapping,
                executor,
                failureHandlerFactories);
        sources.add(source);
        return source.getSource();
    }

    private JMSContext createStandardJmsContext(JmsConnectorCommonConfiguration config) {
        String factoryName = config.getConnectionFactoryName().orElse(null);
        ConnectionFactory factory = pickTheFactory(factoryName);
        JMSContext context = createContext(factory,
                config.getUsername().orElse(null),
                config.getPassword().orElse(null),
                config.getSessionMode());
        config.getClientId().ifPresent(context::setClientID);
        return context;
    }

    private JMSContext createJmsContext(JmsConnectorCommonConfiguration config) {
        if (config.getReuseJmsContext()) {
            return createReuseableJmsContext(config);
        } else {
            return createStandardJmsContext(config);
        }
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        JmsConnectorOutgoingConfiguration oc = new JmsConnectorOutgoingConfiguration(config);
        JmsResourceHolder<JMSProducer> holder = new JmsResourceHolder<>(oc.getChannel(), () -> createJmsContext(oc));
        contexts.add(holder);
        return new JmsSink(holder, oc, openTelemetryInstance, jsonMapping, executor).getSink();
    }

    private ConnectionFactory pickTheFactory(String factoryName) {
        if (factories.isUnsatisfied()) {
            if (factoryName == null) {
                throw ex.illegalStateCannotFindFactory();
            } else {
                throw ex.illegalStateCannotFindNamedFactory(factoryName);
            }
        }

        Iterator<ConnectionFactory> iterator;
        if (factoryName == null) {
            iterator = factories.iterator();
        } else {
            Instance<ConnectionFactory> matching = factories.select(Identifier.Literal.of(factoryName));
            if (matching.isUnsatisfied()) {
                // this `if` block should be removed when support for the `@Named` annotation is removed
                matching = factories.select(NamedLiteral.of(factoryName));
                if (!matching.isUnsatisfied()) {
                    ProviderLogging.log.deprecatedNamed();
                }
            }
            iterator = matching.iterator();
        }

        if (!iterator.hasNext()) {
            if (factoryName == null) {
                throw ex.illegalStateCannotFindFactory();
            } else {
                throw ex.illegalStateCannotFindNamedFactory(factoryName);
            }
        }

        return iterator.next();
    }

    private JMSContext createContext(ConnectionFactory factory, String username, String password, String mode) {
        int sessionMode = parseSessionMode(mode);

        if (username != null) {
            return factory.createContext(username, password, sessionMode);
        } else {
            return factory.createContext(sessionMode);
        }
    }

    private int parseSessionMode(String mode) {
        switch (mode.toUpperCase()) {
            case "AUTO_ACKNOWLEDGE":
                return JMSContext.AUTO_ACKNOWLEDGE;
            case "SESSION_TRANSACTED":
                return JMSContext.SESSION_TRANSACTED;
            case "CLIENT_ACKNOWLEDGE":
                return JMSContext.CLIENT_ACKNOWLEDGE;
            case "DUPS_OK_ACKNOWLEDGE":
                return JMSContext.DUPS_OK_ACKNOWLEDGE;
            default:
                throw ex.illegalStateUnknowSessionMode(mode);
        }
    }

    private ParentContextHolder findOrCreateParentContext(JmsConnectorCommonConfiguration config) {
        String factoryName = config.getConnectionFactoryName().orElse(null);
        String username = config.getUsername().orElse(null);
        String password = config.getPassword().orElse(null);
        String sessionMode = config.getSessionMode();
        String clientId = config.getClientId().orElse(null);

        // Clean up any closed contexts first
        cleanupClosedParentContexts();

        // Look for existing parent context with matching configuration
        for (ParentContextHolder holder : parentContexts) {
            if (holder.matches(factoryName, username, sessionMode, clientId)) {
                // Verify the context is still usable
                if (isContextUsable(holder.getContext())) {
                    log.debug("Reusing existing parent JMS context for channel configuration");
                    return holder;
                } else {
                    // Context is no longer usable, remove it
                    holder.close();
                    parentContexts.remove(holder);
                    log.debug("Removed unusable parent JMS context");
                }
            }
        }

        // Create new parent context if none found
        ConnectionFactory factory = pickTheFactory(factoryName);
        JMSContext parentContext = createContext(factory, username, password, sessionMode);
        config.getClientId().ifPresent(parentContext::setClientID);

        ParentContextHolder holder = new ParentContextHolder(parentContext, factoryName, username, sessionMode, clientId);
        parentContexts.add(holder);
        return holder;
    }

    private JMSContext createReuseableJmsContext(JmsConnectorCommonConfiguration config) {
        // Find or create a parent context for reuse
        ParentContextHolder parentHolder = findOrCreateParentContext(config);

        // Create a child context from the parent
        int sessionMode = parseSessionMode(config.getSessionMode());
        return parentHolder.context.createContext(sessionMode);
    }

    /**
     * Removes closed parent contexts from the list to prevent memory leaks
     */
    private void cleanupClosedParentContexts() {
        parentContexts.removeIf(holder -> {
            if (holder.isClosed()) {
                log.debug("Removing closed parent context from pool");
                return true;
            }
            return false;
        });
    }

    /**
     * Verifies that a JMS context is still usable by attempting a lightweight operation
     */
    private boolean isContextUsable(JMSContext context) {
        try {
            // Attempt to get the session mode - this is a lightweight operation
            // that will fail if the context/connection is closed or broken
            context.getSessionMode();
            return true;
        } catch (Exception e) {
            log.debug("JMS context is no longer usable", e);
            return false;
        }
    }

    private static class ParentContextHolder {
        private final JMSContext context;
        private final String factoryName;
        private final String username;
        private final String sessionMode;
        private final String clientId;
        private volatile boolean closed = false;

        ParentContextHolder(JMSContext context, String factoryName, String username,
                String sessionMode, String clientId) {
            this.context = context;
            this.factoryName = factoryName;
            this.username = username;
            this.sessionMode = sessionMode;
            this.clientId = clientId;
        }

        JMSContext getContext() {
            if (closed) {
                throw new IllegalStateException("Parent context has been closed");
            }
            return context;
        }

        boolean matches(String factoryName, String username, String sessionMode, String clientId) {
            if (closed) {
                return false;
            }

            return Objects.equals(this.factoryName, factoryName) &&
                    Objects.equals(this.username, username) &&
                    Objects.equals(this.sessionMode, sessionMode) &&
                    Objects.equals(this.clientId, clientId);
        }

        void close() {
            if (!closed) {
                closed = true;
                try {
                    context.close();
                } catch (Exception e) {
                    log.warn("Error closing parent JMS context", e);
                }
            }
        }

        boolean isClosed() {
            return closed;
        }
    }
}
