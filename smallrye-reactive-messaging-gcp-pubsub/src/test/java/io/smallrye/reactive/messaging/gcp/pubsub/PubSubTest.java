package io.smallrye.reactive.messaging.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import com.google.pubsub.v1.ProjectTopicName;

import io.smallrye.mutiny.Multi;

public class PubSubTest extends PubSubTestBase {

    private WeldContainer container;
    private String topic = "pubsub-test";
    private String subscription = "pubsub-subscription-test";

    @Before
    public void initTest() {
        final Weld weld = baseWeld();
        weld.addBeanClass(ConsumptionBean.class);
        addConfig(createSourceConfig(topic, subscription));
        container = weld.initialize();
    }

    @After
    public void clearTest() {
        clear();

        if (container != null) {
            container.shutdown();
        }

        CONTAINER.close();
    }

    @Test
    public void testSourceAndSink() throws Exception {
        final ConsumptionBean consumptionBean = container.select(ConsumptionBean.class).get();

        // wait until the subscription is ready
        final PubSubManager manager = container.select(PubSubManager.class).get();
        await().until(() -> manager
                .topicAdminClient(
                        new PubSubConfig(PROJECT_ID, topic, null, true, "localhost", CONTAINER.getMappedPort(PUBSUB_PORT)))
                .listTopicSubscriptions(ProjectTopicName.of(PROJECT_ID, topic))
                .getPage()
                .getPageElementCount() > 0);

        send("Hello-0", topic);
        await().until(() -> consumptionBean.getMessages().size() == 1);
        assertThat(consumptionBean.getMessages().get(0)).isEqualTo("Hello-0");
        for (int i = 1; i < 11; i++) {
            send("Hello-" + i, topic);
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
        final MapBasedConfig config = createSourceConfig(topic, null);
        config.setValue("topic", topic);
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

    private MapBasedConfig createSourceConfig(final String topic, final String subscription) {
        final String prefix = "mp.messaging.incoming.source.";
        final Map<String, Object> config = new HashMap<>();
        config.put(prefix.concat("connector"), PubSubConnector.CONNECTOR_NAME);
        config.put(prefix.concat("topic"), topic);

        if (subscription != null) {
            config.put(prefix.concat("subscription"), subscription);
        }

        // connector properties
        config.put("gcp-pubsub-project-id", PROJECT_ID);
        config.put("mock-pubsub-topics", true);
        config.put("mock-pubsub-host", "localhost");
        config.put("mock-pubsub-port", CONTAINER.getMappedPort(PUBSUB_PORT));

        return new MapBasedConfig(config);
    }
}
