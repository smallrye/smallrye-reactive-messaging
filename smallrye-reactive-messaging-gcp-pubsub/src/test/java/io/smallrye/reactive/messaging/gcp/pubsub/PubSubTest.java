package io.smallrye.reactive.messaging.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.TopicName;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PubSubTest extends PubSubTestBase {

    private WeldContainer container;

    private String topic;

    @BeforeEach
    public void initTest(TestInfo testInfo) {
        topic = testInfo.getTestMethod().map(Method::getName).orElse("") + "_" + UUID.randomUUID();
        initConfiguration(topic);
        final Weld weld = baseWeld();
        weld.addBeanClass(ConsumptionBean.class);
        addConfig(createSourceConfig(topic, SUBSCRIPTION, PUBSUB_CONTAINER.getFirstMappedPort()));
        container = weld.initialize();
    }

    @AfterEach
    public void afterEach() {
        if (container != null) {
            PubSubManager manager = container.select(PubSubManager.class).get();
            deleteTopicIfExists(manager, topic);
            container.shutdown();
        }
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        clear();
    }

    @Test
    public void testSourceAndSink() {
        final ConsumptionBean consumptionBean = container.select(ConsumptionBean.class).get();

        // wait until the subscription is ready
        final PubSubManager manager = container.select(PubSubManager.class).get();
        await().until(() -> manager
                .topicAdminClient(config)
                .listTopicSubscriptions((TopicName) ProjectTopicName.of(PROJECT_ID, topic))
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
        final Flow.Subscriber<? extends Message<?>> subscriber = createSinkSubscriber(topic);
        Multi.createFrom().item(message)
                .map(Message::of)
                .subscribe((Flow.Subscriber<Message<String>>) subscriber);
    }

    private Flow.Subscriber<? extends Message<?>> createSinkSubscriber(final String topic) {
        final MapBasedConfig config = createSourceConfig(topic, null, PUBSUB_CONTAINER.getFirstMappedPort());
        config.put("topic", topic);
        config.write();

        return getConnector().getSubscriber(config);
    }

    private PubSubConnector getConnector() {
        return container.select(PubSubConnector.class, ConnectorLiteral.of(PubSubConnector.CONNECTOR_NAME)).get();
    }

}
