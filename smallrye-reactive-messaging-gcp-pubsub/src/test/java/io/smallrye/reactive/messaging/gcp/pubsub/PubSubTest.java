package io.smallrye.reactive.messaging.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.annotation.Annotation;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.TopicName;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PubSubTest extends PubSubTestBase {

    private WeldContainer container;

    private static final String TOPIC = "pubsub-test";

    @BeforeEach
    public void initTest() {
        initConfiguration(TOPIC);
        final Weld weld = baseWeld();
        weld.addBeanClass(ConsumptionBean.class);
        addConfig(createSourceConfig(TOPIC, SUBSCRIPTION, PUBSUB_CONTAINER.getFirstMappedPort()));
        container = weld.initialize();
    }

    @AfterEach
    public void afterEach() {
        clear();
        PubSubManager manager = container.select(PubSubManager.class).get();
        deleteTopicIfExists(manager, TOPIC);
        container.shutdown();
    }

    @Test
    @Disabled("Failing on CI - to be investigated")
    public void testSourceAndSink() {
        final ConsumptionBean consumptionBean = container.select(ConsumptionBean.class).get();

        // wait until the subscription is ready
        final PubSubManager manager = container.select(PubSubManager.class).get();
        await().until(() -> manager
                .topicAdminClient(config)
                .listTopicSubscriptions((TopicName) ProjectTopicName.of(PROJECT_ID, TOPIC))
                .getPage()
                .getPageElementCount() > 0);

        send("Hello-0", TOPIC);
        await().until(() -> consumptionBean.getMessages().size() == 1);
        assertThat(consumptionBean.getMessages().get(0)).isEqualTo("Hello-0");
        for (int i = 1; i < 11; i++) {
            send("Hello-" + i, TOPIC);
        }
        await().until(() -> consumptionBean.getMessages().size() == 11);
        assertThat(consumptionBean.getMessages()).allSatisfy(s -> assertThat(s).startsWith("Hello-"));
    }

    @SuppressWarnings("unchecked")
    private void send(final String message, final String topic) {
        final SubscriberBuilder<? extends Message<?>, Void> subscriber = createSinkSubscriber(topic);
        Multi.createFrom().item(message)
                .map(Message::of)
                .subscribe((Subscriber<Message<String>>) subscriber.build());
    }

    private SubscriberBuilder<? extends Message<?>, Void> createSinkSubscriber(final String topic) {
        final MapBasedConfig config = createSourceConfig(topic, null, PUBSUB_CONTAINER.getFirstMappedPort());
        config.put("topic", topic);
        config.write();

        return getConnector().getSubscriberBuilder(config);
    }

    private PubSubConnector getConnector() {
        return container.select(PubSubConnector.class, new Connector() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return Connector.class;
            }

            @Override
            public String value() {
                return PubSubConnector.CONNECTOR_NAME;
            }
        }).get();
    }

}
