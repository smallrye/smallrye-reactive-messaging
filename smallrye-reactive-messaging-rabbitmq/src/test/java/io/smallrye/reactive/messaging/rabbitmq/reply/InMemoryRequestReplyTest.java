package io.smallrye.reactive.messaging.rabbitmq.reply;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySink;
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
import io.smallrye.reactive.messaging.providers.locals.ContextDecorator;
import io.smallrye.reactive.messaging.providers.metrics.MetricDecorator;
import io.smallrye.reactive.messaging.providers.metrics.MicrometerDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.SmallRyeConfigTestUtil;

public class InMemoryRequestReplyTest {

    private Weld weld;
    private WeldContainer container;

    @BeforeEach
    public void initWeld() {
        weld = new Weld();
        weld.addExtension(new ConfigExtension());
        weld.addExtension(new ReactiveMessagingExtension());

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

        weld.addBeanClass(EmitterFactoryImpl.class);
        weld.addBeanClass(MutinyEmitterFactoryImpl.class);
        weld.addBeanClass(LegacyEmitterFactoryImpl.class);
        weld.addBeanClass(RabbitMQRequestReplyFactory.class);
        weld.addBeanClass(UUIDCorrelationIdHandler.class);

        weld.addBeanClass(RabbitMQConnector.class);
        weld.addBeanClass(MetricDecorator.class);
        weld.addBeanClass(MicrometerDecorator.class);
        weld.addBeanClass(ContextDecorator.class);

        weld.addBeanClass(InMemoryConnector.class);
        weld.disableDiscovery();
    }

    @AfterEach
    public void stopContainer() {
        if (container != null) {
            container.close();
        }
        InMemoryConnector.clear();
        SmallRyeConfigTestUtil.releaseConfig();
    }

    private <T> T runApplication(MapBasedConfig config, Class<T> clazz) {
        weld.addBeanClass(clazz);
        config.write();
        SmallRyeConfigTestUtil.installConfig();
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(clazz).get();
    }

    @Test
    void testRabbitMQRequestReplyWithInMemoryConnector() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.rr-out.connector", InMemoryConnector.CONNECTOR);

        RequestReplyApp application = runApplication(config, RequestReplyApp.class);

        RabbitMQRequestReply<String, String> rr = application.requestReply();
        assertThat(rr).isInstanceOf(NoOpRabbitMQRequestReplyImpl.class);

        ((NoOpRabbitMQRequestReplyImpl<String, String>) rr)
                .setReplyFunction(req -> "rabbit-reply-" + req);

        String reply = rr.request("hello").await().indefinitely();
        assertThat(reply).isEqualTo("rabbit-reply-hello");

        InMemoryConnector connector = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        InMemorySink<String> sink = connector.sink("rr-out");
        assertThat(sink.received()).hasSize(1);
    }

    @Test
    void testRabbitMQRequestReplyPendingRepliesAndComplete() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.rr-out.connector", InMemoryConnector.CONNECTOR);

        RequestReplyApp application = runApplication(config, RequestReplyApp.class);

        assertThat(application.requestReply().getPendingReplies()).isEmpty();
        application.requestReply().complete();
    }

    @ApplicationScoped
    public static class RequestReplyApp {

        @Inject
        @Channel("rr-out")
        RabbitMQRequestReply<String, String> requestReply;

        public RabbitMQRequestReply<String, String> requestReply() {
            return requestReply;
        }
    }
}
