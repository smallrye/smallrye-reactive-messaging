package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.jms.commit.JmsAcknowledgeCommit;
import io.smallrye.reactive.messaging.jms.fault.JmsFailureHandler;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.mutiny.core.Vertx;

@SuppressWarnings("ReactiveStreamsSubscriberImplementation")
public class JmsSourceTest extends JmsTestBase {

    private JMSContext jms;
    private ActiveMQJMSConnectionFactory factory;
    private Vertx vertx;

    @BeforeEach
    public void init() {
        vertx = Vertx.vertx();
        factory = new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
        jms = factory.createContext();
    }

    @AfterEach
    public void close() {
        jms.close();
        factory.close();
    }

    private JmsResourceHolder<JMSConsumer> getResourceHolder(String channelName) {
        return new JmsResourceHolder<>(channelName, () -> jms);
    }

    private JmsSource createSource(JmsResourceHolder<JMSConsumer> holder, MapBasedConfig mapConfig) {
        JmsConnectorIncomingConfiguration config = new JmsConnectorIncomingConfiguration(mapConfig);
        String destinationName = config.getDestination().orElseGet(config::getChannel);
        String type = config.getDestinationType();
        String selector = config.getSelector().orElse(null);
        boolean nolocal = config.getNoLocal();
        boolean durable = config.getDurable();
        holder.configure(
                r -> JmsConnector.getDestination(r.getContext(), destinationName, type),
                r -> durable
                        ? r.getContext().createDurableConsumer((jakarta.jms.Topic) r.getDestination(),
                                destinationName, selector, nolocal)
                        : r.getContext().createConsumer(r.getDestination(), selector, nolocal));
        holder.getClient();
        JmsMessagePoller poller = () -> {
            jakarta.jms.Message received = holder.getClient().receive();
            return received != null ? Message.of(received) : null;
        };
        JmsFailureHandler.Factory failFactory = failureHandlerFactories
                .select(Identifier.Literal.of(config.getFailureStrategy())).get();
        return new JmsSource(vertx, config,
                UnsatisfiedInstance.instance(), null, poller,
                () -> new JmsAcknowledgeCommit(Runnable::run),
                reportFailure -> failFactory.create(null, config, reportFailure),
                null);
    }

    @Test
    public void testWithString() throws JMSException {
        WeldContainer container = prepare();

        RawMessageConsumerBean bean = container.select(RawMessageConsumerBean.class).get();
        assertThat(bean.messages()).isEmpty();

        Queue q = jms.createQueue("queue-one");
        JMSProducer producer = jms.createProducer();
        TextMessage message = jms.createTextMessage("hello");
        message.setStringProperty("string", "value");
        message.setBooleanProperty("bool", true);
        message.setLongProperty("long", 100L);
        message.setByteProperty("byte", (byte) 5);
        message.setFloatProperty("float", 5.5f);
        message.setDoubleProperty("double", 10.3);
        message.setIntProperty("int", 23);
        message.setObjectProperty("object", "yop");
        message.setShortProperty("short", (short) 3);
        producer.send(q, message);

        await().until(() -> bean.messages().size() == 1);
        IncomingJmsMessage<?> incomingJmsMessage = bean.messages().get(0);
        IncomingJmsMessageMetadata metadata = incomingJmsMessage.getMetadata(IncomingJmsMessageMetadata.class)
                .orElseThrow(() -> new AssertionError("Metadata expected"));
        assertThat(incomingJmsMessage.getPayload()).isEqualTo("hello");
        assertThat(metadata.getBody(String.class)).isEqualTo("hello");
        assertThat(metadata.propertyExists("string")).isTrue();
        assertThat(metadata.propertyExists("missing")).isFalse();
        assertThat(metadata.getStringProperty("string")).isEqualTo("value");
        assertThat(metadata.getBooleanProperty("bool")).isTrue();
        assertThat(metadata.getLongProperty("long")).isEqualTo(100L);
        assertThat(metadata.getByteProperty("byte")).isEqualTo((byte) 5);
        assertThat(metadata.getFloatProperty("float")).isEqualTo(5.5f);
        assertThat(metadata.getDoubleProperty("double")).isEqualTo(10.3);
        assertThat(metadata.getIntProperty("int")).isEqualTo(23);
        assertThat(metadata.getObjectProperty("object")).isInstanceOf(String.class);
        assertThat(((String) message.getObjectProperty("object"))).isEqualTo("yop");
        assertThat(message.getShortProperty("short")).isEqualTo((short) 3);
    }

    @Test
    public void testWithLong() {
        WeldContainer container = prepare();

        RawMessageConsumerBean bean = container.select(RawMessageConsumerBean.class).get();
        assertThat(bean.messages()).isEmpty();

        Queue q = jms.createQueue("queue-one");
        JMSProducer producer = jms.createProducer();
        producer.send(q, 10000L);

        await().until(() -> bean.messages().size() == 1);
        IncomingJmsMessage<?> incomingJmsMessage = bean.messages().get(0);
        assertThat(incomingJmsMessage.getPayload()).isEqualTo(10000L);
    }

    @Test
    public void testWithDisconnection() {
        WeldContainer container = prepare();

        RawMessageConsumerBean bean = container.select(RawMessageConsumerBean.class).get();
        assertThat(bean.messages()).isEmpty();

        Queue q = jms.createQueue("queue-one");
        JMSProducer producer = jms.createProducer();
        producer.send(q, 10000L);
        producer.send(q, 20000L);

        await().untilAsserted(() -> assertThat(bean.messages()).hasSize(2)
                .extracting(m -> (Long) m.getPayload())
                .containsExactly(10000L, 20000L));

        stopArtemis();
        startArtemis();

        init();
        q = jms.createQueue("queue-one");
        producer = jms.createProducer();
        producer.send(q, 30000L);
        producer.send(q, 40000L);

        await().untilAsserted(() -> assertThat(bean.messages()).hasSize(4)
                .extracting(m -> (Long) m.getPayload())
                .containsExactly(10000L, 20000L, 30000L, 40000L));
    }

    @Test
    public void testWithDurableTopic() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "my-topic");
        map.put("mp.messaging.incoming.jms.durable", "true");
        map.put("mp.messaging.incoming.jms.client-id", "me");
        map.put("mp.messaging.incoming.jms.destination-type", "topic");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(RawMessageConsumerBean.class);
        RawMessageConsumerBean bean = container.select(RawMessageConsumerBean.class).get();
        assertThat(bean.messages()).isEmpty();

        Topic q = jms.createTopic("my-topic");
        JMSProducer producer = jms.createProducer();
        String uuid = UUID.randomUUID().toString();
        producer.send(q, uuid);

        await().until(() -> bean.messages().size() == 1);
        IncomingJmsMessage<?> incomingJmsMessage = bean.messages().get(0);
        assertThat(incomingJmsMessage.getPayload()).isEqualTo(uuid);
    }

    @Test
    public void testReceptionOfMultipleMessages() {
        WeldContainer container = prepare();

        RawMessageConsumerBean bean = container.select(RawMessageConsumerBean.class).get();
        assertThat(bean.messages()).isEmpty();

        Queue q = jms.createQueue("queue-one");
        JMSProducer producer = jms.createProducer();

        new Thread(() -> {
            for (int i = 0; i < 50; i++) {
                TextMessage message = jms.createTextMessage(Integer.toString(i));
                producer.send(q, message);
            }
        }).start();

        await().until(() -> bean.messages().size() == 50);
    }

    @Test
    public void testMultipleRequests() {
        JmsResourceHolder<JMSConsumer> holder = getResourceHolder("queue");
        JmsSource source = createSource(holder, new MapBasedConfig().put("channel-name", "queue"));
        Publisher<? extends IncomingJmsMessage<?>> publisher = source.getSource();

        new Thread(() -> {
            JMSContext context = factory.createContext();
            JMSProducer producer = context.createProducer();
            Queue q = context.createQueue("queue");
            for (int i = 0; i < 50; i++) {
                producer.send(q, i);
            }
        }).start();

        List<IncomingJmsMessage<?>> list = new CopyOnWriteArrayList<>();
        AtomicReference<Subscription> upstream = new AtomicReference<>();
        publisher.subscribe(new Subscriber<IncomingJmsMessage<?>>() {
            @Override
            public void onSubscribe(Subscription s) {
                upstream.set(s);
            }

            @Override
            public void onNext(IncomingJmsMessage<?> incomingJmsMessage) {
                list.add(incomingJmsMessage);
            }

            @Override
            public void onError(Throwable t) {
                // ignored
            }

            @Override
            public void onComplete() {
                // ignored
            }
        });

        await().untilAtomic(upstream, is(notNullValue()));
        upstream.get().request(10);
        await().until(() -> list.size() == 10);
        upstream.get().request(4);
        await().until(() -> list.size() == 14);
        upstream.get().request(Long.MAX_VALUE);
        await().until(() -> list.size() == 50);
        assertThat(list.stream().map(r -> (Integer) r.getPayload()).collect(Collectors.toList()))
                .containsAll(IntStream.of(49).boxed().collect(Collectors.toList()));

        source.close();
    }

    @Test
    public void testBroadcast() {
        JmsResourceHolder<JMSConsumer> holder = getResourceHolder("queue");
        JmsSource source = createSource(holder, new MapBasedConfig()
                .with("channel-name", "queue").with("broadcast", true));
        Flow.Publisher<? extends IncomingJmsMessage<?>> publisher = source.getSource();

        List<IncomingJmsMessage<?>> list1 = new ArrayList<>();
        List<IncomingJmsMessage<?>> list2 = new ArrayList<>();

        Multi.createFrom().publisher(publisher).subscribe().with(list1::add);

        new Thread(() -> {
            JMSContext context = factory.createContext();
            JMSProducer producer = context.createProducer();
            Queue q = context.createQueue("queue");
            for (int i = 0; i < 50; i++) {
                producer.send(q, i);
            }
        }).start();

        Multi.createFrom().publisher(publisher).subscribe().with(list2::add);

        await().until(() -> list1.size() == 50);
        await().until(() -> list2.size() == 50);

        source.close();

        assertThat(list1.stream().map(r -> (Integer) r.getPayload()).collect(Collectors.toList()))
                .containsAll(IntStream.of(49).boxed().collect(Collectors.toList()));
        assertThat(list2.stream().map(r -> (Integer) r.getPayload()).collect(Collectors.toList()))
                .containsAll(IntStream.of(49).boxed().collect(Collectors.toList()));
    }

    private WeldContainer prepare() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        return deploy(RawMessageConsumerBean.class);
    }
}
