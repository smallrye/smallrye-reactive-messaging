package io.smallrye.reactive.messaging.pulsar.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class GenericRecordConverterTest extends WeldTestBase {

    public static final int NUMBER_OF_MESSAGES = 10;

    @Test
    public void testBeanUsingConverter() throws PulsarClientException {
        addBeans(GenericRecordConverter.class);
        MyBean bean = runApplication(config(), MyBean.class);

        send(client.newProducer(Schema.AUTO_PRODUCE_BYTES(Schema.STRING))
                .producerName(topic + "-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES,
                (i, p) -> p.newMessage()
                        .key(i % 2 == 0 ? "key" : "k")
                        .value(("v-" + i).getBytes()));

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list())
                .hasSize(10)
                .allSatisfy(r -> assertThat(r.getNativeObject()).asString().startsWith("v-"));
    }

    @Test
    public void testChannelBeanUsingConverter() throws PulsarClientException {
        addBeans(GenericRecordConverter.class);
        MyChannelBean bean = runApplication(config(), MyChannelBean.class);

        bean.consume();

        send(client.newProducer(Schema.AUTO_PRODUCE_BYTES(Schema.STRING))
                .producerName(topic + "-producer")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage()
                        .key(i % 2 == 0 ? "key" : "k")
                        .value(("v-" + i).getBytes()));

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list())
                .hasSize(10)
                .allSatisfy(r -> assertThat(r.getNativeObject()).asString().startsWith("v-"));
    }

    @Test
    @Disabled
    public void testBeanUsingConverterWithNullKeyAndValue() throws PulsarClientException {
        addBeans(GenericRecordConverter.class);
        MyBean bean = runApplication(config(), MyBean.class);

        send(client.newProducer(Schema.AUTO_PRODUCE_BYTES(Schema.STRING))
                .producerName(topic + "-producer")
                .topic(topic)
                .create(),
                NUMBER_OF_MESSAGES, (i, p) -> p.newMessage().key("key").value(null));

        await().until(() -> bean.list().size() == 10);

        assertThat(bean.list())
                .hasSize(10)
                .allSatisfy(r -> assertThat(r.getNativeObject()).isNull());
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.subscriptionInitialReset", "Earliest");
    }

    @ApplicationScoped
    public static class MyBean {

        private final List<GenericRecord> records = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(GenericRecord record) {
            this.records.add(record);
        }

        public List<GenericRecord> list() {
            return records;
        }

    }

    @ApplicationScoped
    public static class MyChannelBean {

        private final List<GenericRecord> records = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("data")
        Multi<GenericRecord> recordMulti;

        public void consume() {
            recordMulti.subscribe().with(records::add);
        }

        public List<GenericRecord> list() {
            return records;
        }

    }

}
