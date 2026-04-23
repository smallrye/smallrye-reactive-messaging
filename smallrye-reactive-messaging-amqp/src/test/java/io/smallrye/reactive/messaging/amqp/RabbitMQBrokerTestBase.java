package io.smallrye.reactive.messaging.amqp;

import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Context;
import io.vertx.mutiny.core.Vertx;

public class RabbitMQBrokerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger("RabbitMQ");

    private static RabbitMQContainer RABBIT = new RabbitMQContainer();

    protected static String host;
    protected static int port;
    protected static int managementPort;
    protected final static String username = "guest";
    protected final static String password = "guest";
    protected AmqpUsage usage;
    protected ExecutionHolder executionHolder;

    @BeforeAll
    public static void setupMutiny() {
        Infrastructure.setCanCallerThreadBeBlockedSupplier(() -> !Context.isOnEventLoopThread());
    }

    @BeforeAll
    public static void startBroker() {
        int maxAttempts = 5;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                RABBIT.start();
                break;
            } catch (Exception e) {
                if (attempt == maxAttempts) {
                    throw e;
                }
                LOGGER.error("Failed to start RabbitMQ container (attempt {}), retrying...", attempt, e);
                try {
                    Thread.sleep(2000L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }

        port = RABBIT.getPort();
        managementPort = RABBIT.getManagementPort();
        host = RABBIT.getHost();

        System.setProperty("amqp-host", host);
        System.setProperty("amqp-port", Integer.toString(port));
    }

    @AfterAll
    public static void stopBroker() {
        RABBIT.stop();
        System.clearProperty("amqp-host");
        System.clearProperty("amqp-port");
        await().until(() -> !RABBIT.isRunning());
    }

    public static void restartBroker() {
        if (RABBIT.isRunning()) {
            stopBroker();
        }
        RABBIT = new RabbitMQContainer(port);
        startBroker();
    }

    @BeforeEach
    public void setup() {
        executionHolder = new ExecutionHolder(Vertx.vertx());

        usage = new AmqpUsage(executionHolder.vertx(), host, port, username, password);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    @AfterEach
    public void tearDown() {
        usage.close();
        executionHolder.terminate(null);
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        MapBasedConfig.cleanup();
    }

    protected static void createQueue(String name) {
        try {
            String auth = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + ":" + managementPort + "/api/queues/%2f/" + name))
                    .header("Authorization", "Basic " + auth)
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString("{\"durable\":false,\"auto_delete\":true}"))
                    .build();
            HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
                    .send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create queue: " + name, e);
        }
    }

    public boolean isAmqpConnectorReady(WeldContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getReadiness().isOk();
    }

    public boolean isAmqpConnectorAlive(WeldContainer container) {
        HealthCenter health = container.getBeanManager().createInstance().select(HealthCenter.class).get();
        return health.getLiveness().isOk();
    }

    public static class RabbitMQContainer extends GenericContainer<RabbitMQContainer> {

        public RabbitMQContainer() {
            super(DockerImageName.parse("docker.io/rabbitmq:4.2.5-management"));
            withExposedPorts(5672, 15672);
            waitingFor(Wait.forHttp("/api/overview").forPort(15672)
                    .withBasicCredentials("guest", "guest")
                    .withStartupTimeout(Duration.ofSeconds(30)));
        }

        public RabbitMQContainer(int port) {
            this();
            addFixedExposedPort(port, 5672);
        }

        public int getPort() {
            return getMappedPort(5672);
        }

        public int getManagementPort() {
            return getMappedPort(15672);
        }

    }

}
