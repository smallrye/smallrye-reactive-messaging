package io.smallrye.reactive.messaging.invalid;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class InvalidStreamTransformerSignatureTest extends WeldTestBaseWithoutTails {

    @Test
    public void testPayloadToPayload() {
        addBeanClass(Tails.class);
        addBeanClass(PayloadToPayload.class);
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);

    }

    @Test
    public void testPayloadToMessage() {
        addBeanClass(Tails.class);
        addBeanClass(PayloadToMessage.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testMessageToMessage() {
        addBeanClass(Tails.class);
        addBeanClass(MessageToMessage.class);
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);
    }

    @Test
    public void testMessageToPayload() {
        addBeanClass(Tails.class);
        addBeanClass(MessageToPayload.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testPayloadToPayloadBuilders() {
        addBeanClass(Tails.class);
        addBeanClass(PayloadToPayloadBuilders.class);
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);

    }

    @Test
    public void testPayloadToMessageBuilders() {
        addBeanClass(Tails.class);
        addBeanClass(PayloadToMessageBuilders.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testMessageToMessageBuilders() {
        addBeanClass(Tails.class);
        addBeanClass(MessageToMessageBuilders.class);
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);
    }

    @Test
    public void testMessageToPayloadBuilders() {
        addBeanClass(Tails.class);
        addBeanClass(MessageToPayloadBuilders.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class PayloadToPayload {
        @Incoming("i")
        @Outgoing("o")
        public Multi<String> consume(Multi<String> i) {
            return i;
        }
    }

    @ApplicationScoped
    public static class PayloadToPayloadBuilders {
        @Incoming("i")
        @Outgoing("o")
        public PublisherBuilder<String> consume(PublisherBuilder<String> i) {
            return i;
        }
    }

    @ApplicationScoped
    public static class PayloadToMessage {
        @Incoming("i")
        @Outgoing("o")
        public Multi<Message<String>> consume(Multi<String> i) {
            return i.map(Message::of);
        }
    }

    @ApplicationScoped
    public static class PayloadToMessageBuilders {
        @Incoming("i")
        @Outgoing("o")
        public PublisherBuilder<Message<String>> consume(PublisherBuilder<String> i) {
            return i.map(Message::of);
        }
    }

    @ApplicationScoped
    public static class MessageToMessage {
        @Incoming("i")
        @Outgoing("o")
        public Multi<Message<String>> consume(Multi<Message<String>> i) {
            return i.map(m -> m.withPayload(m.getPayload().toLowerCase()));
        }
    }

    @ApplicationScoped
    public static class MessageToMessageBuilders {
        @Incoming("i")
        @Outgoing("o")
        public PublisherBuilder<Message<String>> consume(PublisherBuilder<Message<String>> i) {
            return i.map(m -> m.withPayload(m.getPayload().toLowerCase()));
        }
    }

    @ApplicationScoped
    public static class MessageToPayload {
        @Incoming("i")
        @Outgoing("o")
        public Multi<String> consume(Multi<Message<String>> i) {
            return i.map(Message::getPayload);
        }
    }

    @ApplicationScoped
    public static class MessageToPayloadBuilders {
        @Incoming("i")
        @Outgoing("o")
        public PublisherBuilder<String> consume(PublisherBuilder<Message<String>> i) {
            return i.map(Message::getPayload);
        }
    }

    @ApplicationScoped
    public static class Tails {
        @Outgoing("i")
        public Multi<String> generate() {
            return Multi.createFrom().items("a", "b", "c");
        }

        @Incoming("o")
        public void consume(String s) {
            list.add(s);
        }

        private final List<String> list = new CopyOnWriteArrayList<>();

        public List<String> list() {
            return list;
        }
    }

}
