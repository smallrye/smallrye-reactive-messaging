package io.smallrye.reactive.messaging.eventbus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.eventbus.DeliveryOptions;

public class EventBusSourceTest extends EventbusTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
    }

    @Test
    public void testSource() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        EventBusSource source = new EventBusSource(vertx,
                new VertxEventBusConnectorIncomingConfiguration(new MapBasedConfig(config)));

        List<EventBusMessage<Integer>> messages = new ArrayList<>();
        Multi.createFrom().publisher(source.source().buildRs())
                .onItem().castTo(EventBusMessage.class)
                .subscribe().with(messages::add);
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 10, true, null,
                counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream()
                .map(EventBusMessage::getPayload)
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBroadcast() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put("broadcast", true);
        EventBusSource source = new EventBusSource(vertx,
                new VertxEventBusConnectorIncomingConfiguration(new MapBasedConfig(config)));

        List<EventBusMessage<Integer>> messages1 = new ArrayList<>();
        List<EventBusMessage<Integer>> messages2 = new ArrayList<>();
        Multi<EventBusMessage<Integer>> multi = Multi.createFrom().publisher(source.source().buildRs())
                .onItem().transform(x -> (EventBusMessage<Integer>) x);
        multi.subscribe().with(messages1::add);
        multi.subscribe().with(messages2::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(topic, 10, true, null,
                counter::getAndIncrement)).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);
        await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 10);
        assertThat(messages1.stream().map(EventBusMessage::getPayload)
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(messages2.stream().map(EventBusMessage::getPayload)
                .collect(Collectors.toList()))
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testAcknowledgement() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        config.put("use-reply-as-ack", true);
        EventBusSource source = new EventBusSource(vertx,
                new VertxEventBusConnectorIncomingConfiguration(new MapBasedConfig(config)));

        Multi<? extends EventBusMessage<?>> multi = Multi.createFrom().publisher(source.source().buildRs())
                .onItem().transform(x -> (EventBusMessage<?>) x)
                .onItem().transformToUniAndConcatenate(
                        m -> Uni.createFrom().completionStage(m.ack().toCompletableFuture().thenApply(x -> m)));

        List<EventBusMessage<?>> messages = new ArrayList<>();
        multi.subscribe().with(messages::add);

        AtomicBoolean acked = new AtomicBoolean();
        vertx.eventBus().request(topic, 1)
                .onItem().invoke(rep -> acked.set(true))
                .await().indefinitely();

        await().untilTrue(acked);
        assertThat(messages).isNotEmpty();
    }

    @Test
    public void testMessageHeader() {
        String topic = UUID.randomUUID().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("address", topic);
        EventBusSource source = new EventBusSource(vertx,
                new VertxEventBusConnectorIncomingConfiguration(new MapBasedConfig(config)));

        Multi<? extends EventBusMessage<?>> multi = Multi.createFrom().publisher(source.source().buildRs())
                .onItem().transform(x -> (EventBusMessage<?>) x);

        List<EventBusMessage<?>> messages1 = new ArrayList<>();
        multi.subscribe().with(messages1::add);

        vertx.eventBus().sendAndForget(topic, 1, new DeliveryOptions()
                .addHeader("X-key", "value"));

        await().until(() -> messages1.size() == 1);

        EventBusMessage<?> message = messages1.get(0);
        assertThat(message.getHeader("X-key")).contains("value");
        assertThat(message.getHeaders("X-key")).containsExactly("value");
        assertThat(message.getAddress()).isEqualTo(topic);
        assertThat(message.unwrap()).isNotNull();
        assertThat(message.getReplyAddress()).isEmpty();

    }

    @Test
    public void testABeanConsumingTheEventBusMessages() {
        ConsumptionBean bean = deploy();
        await().until(() -> container.select(MediatorManager.class).get().isInitialized());

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        bean.produce();

        await().atMost(2, TimeUnit.MINUTES).until(() -> list.size() >= 10);
        assertThat(list).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    private ConsumptionBean deploy() {
        Weld weld = baseWeld();
        addConfig(getConfig());
        weld.addBeanClass(ConsumptionBean.class);
        weld.addBeanClass(VertxProducer.class);
        container = weld.initialize();
        return container.getBeanManager().createInstance().select(ConsumptionBean.class).get();
    }

    private MapBasedConfig getConfig() {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "address", "data");
        config.put(prefix + "connector", VertxEventBusConnector.CONNECTOR_NAME);
        return new MapBasedConfig(config);
    }

}
