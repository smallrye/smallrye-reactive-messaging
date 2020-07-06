package io.smallrye.reactive.messaging.amqp;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.*;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.extension.HealthCenter;
import io.vertx.mutiny.core.Vertx;
import repeat.RepeatRule;

public class AmqpTestBase {

    private static AmqpBroker broker = new AmqpBroker();

    @Rule
    public RepeatRule rule = new RepeatRule();

    ExecutionHolder executionHolder;
    final static String host = "127.0.0.1";
    final static int port = 5672;
    final static String username = "artemis";
    final static String password = "artemis";
    AmqpUsage usage;

    @BeforeClass
    public static void startBroker() {
        broker.start();
        System.setProperty("amqp-host", host);
        System.setProperty("amqp-port", Integer.toString(port));
        System.setProperty("amqp-user", username);
        System.setProperty("amqp-pwd", password);
    }

    @AfterClass
    public static void stopBroker() {
        broker.stop();
        System.clearProperty("amqp-host");
        System.clearProperty("amqp-port");
    }

    @Before
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());

        usage = new AmqpUsage(executionHolder.vertx(), host, port, username, password);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.clear();
    }

    @After
    public void tearDown() {
        usage.close();
        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.clear();
    }

    public boolean isAmqpConnectorReady(WeldContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getReadiness().isOk();
    }

    public boolean isAmqpConnectorReady(AmqpConnector connector) {
        return connector.getReadiness().isOk();
    }

    public boolean isAmqpConnectorAlive(WeldContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getLiveness().isOk();
    }

    public boolean isAmqpConnectorAlive(AmqpConnector connector) {
        return connector.getLiveness().isOk();
    }

}
