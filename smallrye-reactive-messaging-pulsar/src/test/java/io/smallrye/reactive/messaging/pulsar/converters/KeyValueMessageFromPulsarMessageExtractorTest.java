package io.smallrye.reactive.messaging.pulsar.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.keyed.KeyedMulti;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;

class KeyValueMessageFromPulsarMessageExtractorTest extends WeldTestBase {

    @Test
    void defaultExctractorStringSchema() throws PulsarClientException {
        addBeans(Sink.class, AppWithDefault.class);

        runApplication(baseConfig()
                .with("mp.messaging.incoming.in.connector", "smallrye-pulsar")
                .with("mp.messaging.incoming.in.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.in.topic", topic)
                .with("mp.messaging.incoming.in.schema", "STRING")
                .with("mp.messaging.incoming.in.subscriptionInitialReset", "Earliest"));

        Sink sink = get(Sink.class);

        sendMessages(client.newProducer(Schema.STRING)
                .producerName("test-producer")
                .topic(topic)
                .create(),
                p -> List.of(
                        p.newMessage().key("a").value("1"),
                        p.newMessage().key("b").value("1"),
                        p.newMessage().key("b").value("2"),
                        p.newMessage().key("a").value("2"),
                        p.newMessage().key("a").value("3"),
                        p.newMessage().key("c").value("1"),
                        p.newMessage().key("c").value("2"),
                        p.newMessage().key("a").value("4")));

        await().until(() -> sink.list().size() == 11);
        assertThat(sink.list())
                .containsExactlyInAnyOrder(
                        "A-0", "B-0", "C-0",
                        "A-1", "A-2", "A-3", "A-4",
                        "B-1", "B-2", "C-1", "C-2");
    }

    @Test
    void defaultExtractorKeyValueSchema() throws PulsarClientException {
        addBeans(Sink.class, AppWithDefaultKeyValue.class);

        runApplication(baseConfig()
                .with("mp.messaging.incoming.in.connector", "smallrye-pulsar")
                .with("mp.messaging.incoming.in.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.in.topic", topic)
                .with("mp.messaging.incoming.in.subscriptionInitialReset", "Earliest"));

        Sink sink = get(Sink.class);

        sendMessages(client.newProducer(Schema.KeyValue(Schema.STRING, Schema.INT32))
                .producerName("test-producer")
                .topic(topic)
                .create(),
                p -> List.of(
                        p.newMessage().value(new KeyValue<>("a", 1)),
                        p.newMessage().value(new KeyValue<>("b", 1)),
                        p.newMessage().value(new KeyValue<>("b", 2)),
                        p.newMessage().value(new KeyValue<>("a", 2)),
                        p.newMessage().value(new KeyValue<>("a", 3)),
                        p.newMessage().value(new KeyValue<>("c", 1)),
                        p.newMessage().value(new KeyValue<>("c", 2)),
                        p.newMessage().value(new KeyValue<>("a", 4))));

        await().until(() -> sink.list().size() == 11);
        assertThat(sink.list())
                .containsExactlyInAnyOrder(
                        "A-0", "B-0", "C-0",
                        "A-1", "A-2", "A-3", "A-4",
                        "B-1", "B-2", "C-1", "C-2");
    }

    @ApplicationScoped
    public static class Sink {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("out")
        void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    public static class Container<T> {
        Message<T> message;
        int count;

        public Container(Message<T> message, int count) {
            this.message = message;
            this.count = count;
        }
    }

    @ApplicationScoped
    public static class AppWithDefault {

        @Incoming("in")
        @Outgoing("out")
        public Multi<Message<String>> reshape(KeyedMulti<String, Message<String>> keyed) {
            assertThat(keyed.key()).isNotNull().isNotBlank();
            return keyed
                    .select().distinct()
                    .onItem().scan(() -> new Container<String>(Message.of(null), 0),
                            (cont, msg) -> new Container<>(msg, cont.count + 1))
                    .map(cont -> cont.message.withPayload(keyed.key().toUpperCase() + "-" + cont.count));
        }

    }

    @ApplicationScoped
    public static class AppWithDefaultKeyValue {

        @Produces
        @Identifier("in")
        Schema<KeyValue<String, Integer>> schema = Schema.KeyValue(Schema.STRING, Schema.INT32);

        @Incoming("in")
        @Outgoing("out")
        public Multi<Message<String>> reshape(KeyedMulti<String, Message<Integer>> keyed) {
            assertThat(keyed.key()).isNotNull().isNotBlank();
            return keyed
                    .select().distinct()
                    .onItem().scan(() -> new Container<Integer>(Message.of(null), 0),
                            (cont, msg) -> new Container<>(msg, cont.count + 1))
                    .map(cont -> cont.message.withPayload(keyed.key().toUpperCase() + "-" + cont.count));
        }

    }
}
