package io.smallrye.reactive.messaging.pulsar.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.config.inject.ConfigExtension;
import io.smallrye.mutiny.Uni;
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
import io.smallrye.reactive.messaging.providers.metrics.MetricDecorator;
import io.smallrye.reactive.messaging.providers.metrics.MicrometerDecorator;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;
import io.smallrye.reactive.messaging.pulsar.ConfigResolver;
import io.smallrye.reactive.messaging.pulsar.PulsarClientServiceImpl;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.SchemaResolver;
import io.smallrye.reactive.messaging.pulsar.ack.PulsarMessageAck;
import io.smallrye.reactive.messaging.pulsar.fault.PulsarNack;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.smallrye.reactive.messaging.test.common.config.SmallRyeConfigTestUtil;

public class InMemoryTransactionalProducerTest {

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
        weld.addBeanClass(PulsarTransactionsFactory.class);

        weld.addBeanClass(PulsarConnector.class);
        weld.addBeanClass(SchemaResolver.class);
        weld.addBeanClass(ConfigResolver.class);
        weld.addBeanClass(PulsarClientServiceImpl.class);
        weld.addBeanClass(PulsarMessageAck.Factory.class);
        weld.addBeanClass(PulsarNack.Factory.class);
        weld.addBeanClass(MetricDecorator.class);
        weld.addBeanClass(MicrometerDecorator.class);

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

    private BeanManager getBeanManager() {
        return container.getBeanManager();
    }

    @Test
    void testPulsarTransactionsWithInMemoryConnector() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.tx-out.connector", InMemoryConnector.CONNECTOR);

        TransactionalProducerApp application = runApplication(config, TransactionalProducerApp.class);

        application.produceInTransaction(5).await().indefinitely();

        InMemoryConnector connector = getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        InMemorySink<Integer> sink = connector.sink("tx-out");

        List<? extends Message<Integer>> received = sink.received();
        assertThat(received).hasSize(5);
        assertThat(received).extracting(Message::getPayload).containsExactly(0, 1, 2, 3, 4);
    }

    @ApplicationScoped
    public static class TransactionalProducerApp {

        @Inject
        @Channel("tx-out")
        PulsarTransactions<Integer> transaction;

        Uni<Void> produceInTransaction(int count) {
            return transaction.withTransaction(emitter -> {
                for (int i = 0; i < count; i++) {
                    emitter.send(i);
                }
                return Uni.createFrom().voidItem();
            });
        }
    }
}
