package io.smallrye.reactive.messaging.aws.sqs;

import static org.awaitility.Awaitility.await;

import java.util.HashSet;

import jakarta.enterprise.inject.spi.BeanManager;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;

import io.micrometer.core.instrument.Counter;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.providers.MediatorFactory;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.providers.extension.EmitterFactoryImpl;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.providers.extension.LegacyEmitterFactoryImpl;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterFactoryImpl;
import io.smallrye.reactive.messaging.providers.extension.ReactiveMessagingExtension;
import io.smallrye.reactive.messaging.providers.impl.ConfiguredChannelFactory;
import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;
import io.smallrye.reactive.messaging.providers.metrics.MetricDecorator;
import io.smallrye.reactive.messaging.providers.metrics.MicrometerDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

class SqsConnectorTest {

    protected Weld weld;
    protected WeldContainer container;

    DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:3.1.0");

    @Rule
    public LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(Service.SQS);

    @BeforeEach
    public void initWeld() {
        weld = new Weld();

        // SmallRye config
        ConfigExtension extension = new ConfigExtension();
        weld.addExtension(extension);

        weld.addBeanClass(MediatorFactory.class);
        weld.addBeanClass(MediatorManager.class);
        weld.addBeanClass(InternalChannelRegistry.class);
        weld.addBeanClass(ConnectorFactories.class);
        weld.addBeanClass(ConfiguredChannelFactory.class);
        weld.addBeanClass(ChannelProducer.class);
        weld.addBeanClass(ExecutionHolder.class);
        weld.addBeanClass(WorkerPoolRegistry.class);
        weld.addBeanClass(HealthCenter.class);
        weld.addBeanClass(Wiring.class);
        weld.addExtension(new ReactiveMessagingExtension());

        weld.addBeanClass(EmitterFactoryImpl.class);
        weld.addBeanClass(MutinyEmitterFactoryImpl.class);
        weld.addBeanClass(LegacyEmitterFactoryImpl.class);

        weld.addBeanClass(SqsConnector.class);
        weld.addBeanClass(MetricDecorator.class);
        weld.addBeanClass(MicrometerDecorator.class);
        weld.addBeanClass(SqsManager.class);
        weld.addBeanClass(MetricRegistry.class);
        weld.addBeanClass(Counter.class);
        weld.disableDiscovery();
    }

    @BeforeEach
    void setupLocalstack() {
        localstack.start();
        System.setProperty("aws.accessKeyId", localstack.getAccessKey());
        System.setProperty("aws.secretAccessKey", localstack.getSecretKey());
    }

    public BeanManager getBeanManager() {
        if (container == null) {
            runApplication(new MapBasedConfig());
        }
        return container.getBeanManager();
    }

    public <T> T get(Class<T> clazz) {
        return getBeanManager().createInstance().select(clazz).get();
    }

    public void runApplication(MapBasedConfig config) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.cleanup();
        }
        container = weld.initialize();
    }

    public <T> T runApplication(MapBasedConfig config, Class<T> clazz) {
        weld.addBeanClass(clazz);
        runApplication(config);
        return get(clazz);
    }

    @AfterEach
    public void stopContainer() {
        if (container != null) {
            var connector = getBeanManager().createInstance()
                    .select(SqsConnector.class, ConnectorLiteral.of(SqsConnector.CONNECTOR_NAME))
                    .get();
            connector.close();
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        if (localstack.isRunning() || localstack.isCreated()) {
            localstack.close();
        }
    }

    @Test
    void testConsumer() {
        var queue = "test-queue-for-consuming-data";
        try (var sqsClient = SqsClient.builder().endpointOverride(localstack.getEndpoint())
                .credentialsProvider(SystemPropertyCredentialsProvider.create())
                .region(Region.of(localstack.getRegion()))
                .build()) {
            var queueUrl = sqsClient.createQueue(r -> r.queueName(queue)).queueUrl();
            sqsClient.sendMessage(r -> r.queueUrl(queueUrl).messageBody("hello"));
            MapBasedConfig config = new MapBasedConfig().with(
                    "mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                    .with("mp.messaging.incoming.data.queue", queue)
                    .with("mp.messaging.incoming.data.endpointOverride",
                            localstack.getEndpoint().toString())
                    .with("mp.messaging.incoming.data.region", localstack.getRegion())
                    .with("mp.messaging.incoming.data.credentialsProvider",
                            "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider");

            ConsumerApp app = runApplication(config, ConsumerApp.class);
            int expected = 1;
            await().until(() -> app.received().size() == expected);
        }
    }

    @Test
    void testProducer() {
        var queue = "test-queue-for-producer";
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.queue", queue)
                .with("mp.messaging.outgoing.data.endpointOverride",
                        localstack.getEndpoint().toString())
                .with("mp.messaging.outgoing.data.region", localstack.getRegion())
                .with("mp.messaging.outgoing.data.connector", SqsConnector.CONNECTOR_NAME);

        int expected = 10;
        try (var sqsClient = SqsClient.builder().endpointOverride(localstack.getEndpoint())
                .credentialsProvider(SystemPropertyCredentialsProvider.create())
                .region(Region.of(localstack.getRegion()))
                .build()) {
            var queueUrl = sqsClient.createQueue(r -> r.queueName(queue)).queueUrl();
            var app = runApplication(config, ProducerApp.class);
            var received = new HashSet<String>();
            await().until(() -> {
                sqsClient.receiveMessage(r -> r.queueUrl(queueUrl).maxNumberOfMessages(10))
                        .messages().forEach(m -> {
                            received.add(m.body());
                        });
                return received.size() == expected;
            });
        }
    }
}
