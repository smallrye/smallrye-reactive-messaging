package io.smallrye.reactive.messaging.jms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.*;

import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Queue;

import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

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
        assertThat(bean.list()).hasSizeGreaterThan(3);
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
        assertThat(bean.list()).hasSizeGreaterThan(3);
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
        await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> bean.list().size() >= 1);

        List<IncomingJmsMessage<Integer>> messages = bean.messages();
        messages.forEach(msg -> {
            IncomingJmsMessageMetadata metadata = msg.getMetadata(IncomingJmsMessageMetadata.class)
                    .orElseThrow(() -> new AssertionError("Metadata expected"));
            assertThat(metadata.getDeliveryMode()).isEqualTo(DeliveryMode.PERSISTENT);
            assertThat(metadata.getCorrelationId()).isNull();
            assertThat(metadata.getDestination()).isInstanceOf(Queue.class);
            assertThat(metadata.getDeliveryTime()).isNotNegative();
            assertThat(metadata.getPriority()).isEqualTo(4);
            assertThat(metadata.getMessageId()).isNotNull();
            assertThat(metadata.getTimestamp()).isPositive();
            Enumeration<String> names = metadata.getPropertyNames();
            List<String> list = new ArrayList<>();
            while (names.hasMoreElements()) {
                list.add(names.nextElement());
            }
            assertThat(list).hasSize(2).contains("_classname");
            assertThat(metadata.isRedelivered()).isFalse();
            assertThat(metadata.getReplyTo()).isNull();
            assertThat(metadata.getType()).isNotNull();
            assertThat(metadata.getExpiration()).isEqualTo(0L);
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
        assertThat(bean.list()).isNotEmpty();
    }

    @Test
    public void testInvalidSessionMode() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        map.put("mp.messaging.incoming.jms.session-mode", "invalid");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        assertThatThrownBy(() -> deploy(PayloadConsumerBean.class, ProducerBean.class))
                .isInstanceOf(DeploymentException.class);

    }

    @Test
    public void testWithInvalidIncomingDestinationType() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination-type", "invalid");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        assertThatThrownBy(() -> deploy(PayloadConsumerBean.class, ProducerBean.class))
                .isInstanceOf(DeploymentException.class);

    }

    @Test
    public void testWithInvalidOutgoingDestinationType() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.outgoing.queue-one.destination-type", "invalid");
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination-type", "queue");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        assertThatThrownBy(() -> {
            deploy(PayloadConsumerBean.class, ProducerBean.class);
        }).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testWithStringReuseConnection() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.outgoing.queue-one.reuse-jms-context", Boolean.TRUE.toString());
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.reuse-jms-context", Boolean.TRUE.toString());
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class, ProducerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();
        await().until(() -> bean.list().size() > 3);
        assertThat(bean.list()).hasSizeGreaterThan(3);
    }

}
