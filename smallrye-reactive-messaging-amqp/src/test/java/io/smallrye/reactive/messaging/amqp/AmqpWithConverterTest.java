package io.smallrye.reactive.messaging.amqp;

import static org.awaitility.Awaitility.await;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class AmqpWithConverterTest extends AmqpBrokerTestBase {

    private SeContainer container;

    @AfterEach
    public void cleanup() {
        container.close();
    }

    @Test
    public void testAckWithConverter() {
        String address = UUID.randomUUID().toString();
        SeContainerInitializer weld = Weld.newInstance();
        new MapBasedConfig()
                .with("mp.messaging.incoming.in.connector", AmqpConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.host", host)
                .with("mp.messaging.incoming.in.port", port)
                .with("mp.messaging.incoming.in.username", username)
                .with("mp.messaging.incoming.in.password", password)
                .with("mp.messaging.incoming.in.address", address)
                .write();
        weld.addBeanClasses(DummyConverter.class, MyApp.class);
        container = weld.initialize();

        await().until(() -> isAmqpConnectorAlive(container));
        await().until(() -> isAmqpConnectorReady(container));

        AtomicInteger counter = new AtomicInteger();
        usage.produceTenIntegers(address, counter::incrementAndGet);

        MyApp app = container.getBeanManager().createInstance().select(MyApp.class).get();
        await().until(() -> app.list().size() == 10);
    }

    @ApplicationScoped
    public static class DummyConverter implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return true;
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            return in.withPayload(new DummyData());
        }
    }

    @ApplicationScoped
    public static class MyApp {
        List<DummyData> list = new ArrayList<>();

        @Incoming("in")
        public void consume(DummyData data) {
            list.add(data);
        }

        public List<DummyData> list() {
            return list;
        }
    }

    public static class DummyData {

    }
}
