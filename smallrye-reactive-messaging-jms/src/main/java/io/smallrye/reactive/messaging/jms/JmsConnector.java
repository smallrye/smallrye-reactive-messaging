package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;

@ApplicationScoped
@Connector(JmsConnector.CONNECTOR_NAME)

@ConnectorAttribute(name = "connection-factory-name", description = "The name of the JMS connection factory  (`javax.jms.ConnectionFactory`) to be used. If not set, it uses any exposed JMS connection factory", direction = Direction.INCOMING_AND_OUTGOING, type = "String")
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

@ConnectorAttribute(name = "disable-message-id", description = "Omit the message id in the outbound JMS message", direction = Direction.OUTGOING, type = "boolean")
@ConnectorAttribute(name = "disable-message-timestamp", description = "Omit the message timestamp in the outbound JMS message", direction = Direction.OUTGOING, type = "boolean")
@ConnectorAttribute(name = "delivery-mode", description = "The delivery mode. Either `persistent` or `non_persistent`", direction = Direction.OUTGOING, type = "string")
@ConnectorAttribute(name = "delivery-delay", description = "The delivery delay", direction = Direction.OUTGOING, type = "long")
@ConnectorAttribute(name = "ttl", description = "The JMS Message time-to-live", direction = Direction.OUTGOING, type = "long")
@ConnectorAttribute(name = "correlation-id", description = "The JMS Message correlation id", direction = Direction.OUTGOING, type = "string")
@ConnectorAttribute(name = "priority", description = "The JMS Message priority", direction = Direction.OUTGOING, type = "int")
@ConnectorAttribute(name = "reply-to", description = "The reply to destination if any", direction = Direction.OUTGOING, type = "string")
@ConnectorAttribute(name = "reply-to-destination-type", description = "The type of destination for the response. It can be either `queue` or `topic`", direction = Direction.OUTGOING, type = "string", defaultValue = "queue")
public class JmsConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

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
    Instance<ConnectionFactory> factories;

    @Inject
    Instance<Jsonb> jsonb;

    @Inject
    @ConfigProperty(name = "smallrye.jms.threads.max-pool-size", defaultValue = DEFAULT_MAX_POOL_SIZE)
    int maxPoolSize;

    @Inject
    @ConfigProperty(name = "smallrye.jms.threads.ttl", defaultValue = DEFAULT_THREAD_TTL)
    int ttl;

    private ExecutorService executor;
    private Jsonb json;
    private final List<JmsSource> sources = new CopyOnWriteArrayList<>();
    private final List<JMSContext> contexts = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void init() {
        this.executor = new ThreadPoolExecutor(0, maxPoolSize, ttl, TimeUnit.SECONDS, new SynchronousQueue<>());
        if (jsonb.isUnsatisfied()) {
            this.json = JsonbBuilder.create();
        } else {
            this.json = jsonb.get();
        }

    }

    @PreDestroy
    public void cleanup() {
        sources.forEach(JmsSource::close);
        contexts.forEach(JMSContext::close);
        this.executor.shutdown();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        JmsConnectorIncomingConfiguration ic = new JmsConnectorIncomingConfiguration(config);
        JMSContext context = createJmsContext(ic);
        contexts.add(context);
        JmsSource source = new JmsSource(context, ic, json, executor);
        sources.add(source);
        return source.getSource();
    }

    private JMSContext createJmsContext(JmsConnectorCommonConfiguration config) {
        String factoryName = config.getConnectionFactoryName().orElse(null);
        ConnectionFactory factory = pickTheFactory(factoryName);
        JMSContext context = createContext(factory,
                config.getUsername().orElse(null),
                config.getPassword().orElse(null),
                config.getSessionMode());
        config.getClientId().ifPresent(context::setClientID);
        return context;
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        JmsConnectorOutgoingConfiguration oc = new JmsConnectorOutgoingConfiguration(config);
        JMSContext context = createJmsContext(oc);
        contexts.add(context);
        return new JmsSink(context, oc, json, executor).getSink();
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
            iterator = factories.select(NamedLiteral.of(factoryName)).iterator();
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
        int sessionMode;
        switch (mode.toUpperCase()) {
            case "AUTO_ACKNOWLEDGE":
                sessionMode = JMSContext.AUTO_ACKNOWLEDGE;
                break;
            case "SESSION_TRANSACTED":
                sessionMode = JMSContext.SESSION_TRANSACTED;
                break;
            case "CLIENT_ACKNOWLEDGE":
                sessionMode = JMSContext.CLIENT_ACKNOWLEDGE;
                break;
            case "DUPS_OK_ACKNOWLEDGE":
                sessionMode = JMSContext.DUPS_OK_ACKNOWLEDGE;
                break;
            default:
                throw ex.illegalStateUnknowSessionMode(mode);
        }

        if (username != null) {
            return factory.createContext(username, password, sessionMode);
        } else {
            return factory.createContext(sessionMode);
        }
    }
}
