package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;

import javax.enterprise.inject.spi.DeploymentException;
import javax.jms.DeliveryMode;
import javax.jms.Queue;

import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Test;

import io.smallrye.reactive.messaging.jms.support.JmsTestBase;
import io.smallrye.reactive.messaging.jms.support.MapBasedConfig;

public class JmsConnectorTest extends JmsTestBase {

    @Test
    public void testWithString() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class, ProducerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();
        await().until(() -> bean.list().size() > 3);
    }

    @Test
    public void testWithStringAndSessionModel() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        map.put("mp.messaging.incoming.jms.session-mode", "DUPS_OK_ACKNOWLEDGE");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class, ProducerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();
        await().until(() -> bean.list().size() > 3);
    }

    @Test
    public void testWithMessage() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(MessageConsumerBean.class, ProducerBean.class);

        MessageConsumerBean bean = container.select(MessageConsumerBean.class).get();
        await().until(() -> bean.list().size() > 3);

        List<IncomingJmsMessage<Integer>> messages = bean.messages();
        messages.forEach(msg -> {
            assertThat(msg.getJMSDeliveryMode()).isEqualTo(DeliveryMode.PERSISTENT);
            assertThat(msg.getJMSCorrelationID()).isNull();
            assertThat(msg.getJMSCorrelationIDAsBytes()).isNull();
            assertThat(msg.getJMSDestination()).isInstanceOf(Queue.class);
            assertThat(msg.getJMSDeliveryTime()).isNotNull();
            assertThat(msg.getJMSPriority()).isEqualTo(4);
            assertThat(msg.getJMSMessageID()).isNotNull();
            assertThat(msg.getJMSTimestamp()).isPositive();
            Enumeration names = msg.getPropertyNames();
            List<String> list = new ArrayList<>();
            while (names.hasMoreElements()) {
                list.add(names.nextElement().toString());
            }
            assertThat(list).hasSize(2).contains("_classname");
            assertThat(msg.getJMSRedelivered()).isFalse();
            assertThat(msg.getJMSReplyTo()).isNull();
            assertThat(msg.getJMSType()).isNotNull();
            assertThat(msg.getJMSExpiration()).isEqualTo(0L);
        });
    }

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
    }

    @Test(expected = DeploymentException.class)
    public void testInvalidSessionMode() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        map.put("mp.messaging.incoming.jms.session-mode", "invalid");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        deploy(PayloadConsumerBean.class, ProducerBean.class);
    }

    @Test(expected = DeploymentException.class)
    public void testWithInvalidIncomingDestinationType() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination-type", "invalid");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        deploy(PayloadConsumerBean.class, ProducerBean.class);
    }

    @Test(expected = DeploymentException.class)
    public void testWithInvalidOutgoingDestinationType() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.outgoing.queue-one.destination-type", "invalid");
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination-type", "queue");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        deploy(PayloadConsumerBean.class, ProducerBean.class);
    }

}
