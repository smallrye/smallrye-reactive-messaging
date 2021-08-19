package io.smallrye.reactive.messaging.json;

import io.smallrye.reactive.messaging.jms.JmsConnector;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class JmsConnectorTest extends JmsTestBase {

    @Test
    public void testWithPerson() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PersonConsumerBean.class, PersonProducerBean.class);

        PersonConsumerBean bean = container.select(PersonConsumerBean.class).get();
        await().until(() -> bean.list().size() > 1);
        assertThat(bean.list()).isNotEmpty();
    }
}
