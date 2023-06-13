package io.smallrye.reactive.messaging.pulsar.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarMessageConverterTest extends WeldTestBase {

    public static final int NUMBER_OF_MESSAGES = 10;

    @Test
    public void testBeanUsingConverter() throws PulsarClientException {
        addBeans(PulsarMessageConverter.class);
        MyBean bean = runApplication(config(), MyBean.class);

        send(client.newProducer(Schema.STRING)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES,
                (i, p) -> p.newMessage()
                        .key(i % 2 == 0 ? "key" : "k")
                        .value("v-" + i));

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list()).hasSize(10).allSatisfy(r -> {
            assertThat(r.getValue()).startsWith("v-");
            assertThat(r.getKey()).startsWith("k");
            if (!r.getKey().equalsIgnoreCase("key")) {
                assertThat(r.getKey()).isEqualTo("k");
            }
        });
    }

    @Test
    public void testChannelBeanUsingConverter() throws PulsarClientException {
        addBeans(PulsarMessageConverter.class);
        MyChannelBean bean = runApplication(config(), MyChannelBean.class);

        bean.consume();

        send(client.newProducer(Schema.STRING)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage()
                        .key(i % 2 == 0 ? "key" : "k")
                        .value("v-" + i));

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list()).hasSize(10).allSatisfy(r -> {
            assertThat(r.getValue()).startsWith("v-");
            assertThat(r.getKey()).startsWith("k");
            if (!r.getKey().equalsIgnoreCase("key")) {
                assertThat(r.getKey()).isEqualTo("k");
            }
        });
    }

    @Test
    public void testBeanUsingConverterWithNullKeyAndValue() throws PulsarClientException {
        addBeans(PulsarMessageConverter.class);
        MyBean bean = runApplication(config(), MyBean.class);

        send(client.newProducer(Schema.STRING)
                .producerName(topic + "-producer")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().value(null));

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list())
                .hasSize(10)
                .allSatisfy(r -> assertThat(r.getValue()).isNull());
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.schema", "STRING")
                .with("mp.messaging.incoming.data.subscriptionInitialReset", "Earliest");
    }

    @ApplicationScoped
    public static class MyBean {

        private final List<Message<String>> records = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Message<String> record) {
            this.records.add(record);
        }

        public List<Message<String>> list() {
            return records;
        }

    }

    @ApplicationScoped
    public static class MyChannelBean {

        private final List<Message<String>> records = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("data")
        Multi<Message<String>> recordMulti;

        public void consume() {
            recordMulti.subscribe().with(records::add);
        }

        public List<Message<String>> list() {
            return records;
        }

    }

}
