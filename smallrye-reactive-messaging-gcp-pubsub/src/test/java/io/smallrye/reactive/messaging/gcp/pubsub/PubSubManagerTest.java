package io.smallrye.reactive.messaging.gcp.pubsub;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;

import io.smallrye.reactive.messaging.providers.impl.ConnectorFactories;

public class PubSubManagerTest extends PubSubTestBase {

    private static final String TOPIC = "pubsub-manager-test";

    private WeldContainer container;

    @BeforeEach
    public void initTest() {
        initConfiguration(TOPIC);
        final Weld weld = baseWeld();
        addConfig(createSourceConfig(TOPIC, SUBSCRIPTION, PUBSUB_CONTAINER.getFirstMappedPort()));
        weld.addBeanClass(ConnectorFactories.class);
        container = weld.initialize();
    }

    @AfterEach
    public void afterEach() {
        // cleanup
        PubSubManager manager = container.select(PubSubManager.class).get();
        deleteTopicIfExists(manager, TOPIC);
        clear();
        container.shutdown();
    }

    @Test
    public void testResourceCleanup() {
        final PubSubManager manager = container.select(PubSubManager.class).get();

        // create a resource
        try {
            manager.topicAdminClient(config)
                    .createTopic(TopicName.of(PROJECT_ID, TOPIC));
        } catch (io.grpc.StatusRuntimeException e) {
            // already existing, ignore
        }

        // mimic the container destroying and then recreating the resource
        manager.destroy();

        // verify that a new resource can be created with the same config after the resources were disposed
        final Topic topic = manager.topicAdminClient(config)
                .getTopic(TopicName.of(PROJECT_ID, TOPIC));

        assertNotNull(topic);
    }

}
