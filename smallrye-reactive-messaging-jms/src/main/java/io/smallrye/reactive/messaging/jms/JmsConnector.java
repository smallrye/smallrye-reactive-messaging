package io.smallrye.reactive.messaging.jms;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

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

@ApplicationScoped
@Connector(JmsConnector.CONNECTOR_NAME)
public class JmsConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    /**
     * The name of the connector: {@code smallrye-jms}
     */
    static final String CONNECTOR_NAME = "smallrye-jms";

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
        JMSContext context = createJmsContext(config);
        contexts.add(context);
        JmsSource source = new JmsSource(context, config, json, executor);
        sources.add(source);
        return source.getSource();
    }

    private JMSContext createJmsContext(Config config) {
        String factoryName = config.getOptionalValue("connection-factory-name", String.class).orElse(null);
        ConnectionFactory factory = pickTheFactory(factoryName);
        JMSContext context = createContext(factory,
                config.getOptionalValue("username", String.class).orElse(null),
                config.getOptionalValue("password", String.class).orElse(null),
                config.getOptionalValue("session-mode", String.class).orElse("AUTO_ACKNOWLEDGE"));

        config.getOptionalValue("client-id", String.class).ifPresent(context::setClientID);
        return context;
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        JMSContext context = createJmsContext(config);
        contexts.add(context);
        return new JmsSink(context, config, json, executor).getSink();
    }

    private ConnectionFactory pickTheFactory(String factoryName) {
        if (factories.isUnsatisfied()) {
            if (factoryName == null) {
                throw new IllegalStateException("Cannot find a javax.jms.ConnectionFactory bean");
            } else {
                throw new IllegalStateException("Cannot find a javax.jms.ConnectionFactory bean named " + factoryName);
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
                throw new IllegalStateException("Cannot find a javax.jms.ConnectionFactory bean");
            } else {
                throw new IllegalStateException("Cannot find a javax.jms.ConnectionFactory bean named " + factoryName);
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
                throw new IllegalArgumentException("Unknown session mode: " + mode);
        }

        if (username != null) {
            return factory.createContext(username, password, sessionMode);
        } else {
            return factory.createContext(sessionMode);
        }
    }
}
