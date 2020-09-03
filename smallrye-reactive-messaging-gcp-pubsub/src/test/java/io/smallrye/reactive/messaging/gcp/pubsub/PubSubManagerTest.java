package io.smallrye.reactive.messaging.gcp.pubsub;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;

public class PubSubManagerTest extends PubSubTestBase {

    private WeldContainer container;

    @BeforeEach
    public void initTest() {
        final Weld weld = baseWeld();
        addConfig(createSourceConfig(TOPIC, SUBSCRIPTION, PUBSUB_CONTAINER.getFirstMappedPort()));
        container = weld.initialize();
    }

    @AfterEach
    public void afterEach() {
        // cleanup
        PubSubManager manager = container.select(PubSubManager.class).get();
        deleteTopicIfExists(manager);
        clear();
        container.shutdown();
    }

    @Test
    public void testResourceCleanup() {
        final PubSubManager manager = container.select(PubSubManager.class).get();

        // create a resource
        try {
            manager.topicAdminClient(CONFIG)
                    .createTopic(TopicName.of(PROJECT_ID, TOPIC));
        } catch (io.grpc.StatusRuntimeException e) {
            // already existing, ignore
        }

        // mimic the container destroying and then recreating the resource
        manager.destroy();

        // verify that a new resource can be created with the same config after the resources were disposed
        final Topic topic = manager.topicAdminClient(CONFIG)
                .getTopic(TopicName.of(PROJECT_ID, TOPIC));

        assertNotNull(topic);
    }

}
