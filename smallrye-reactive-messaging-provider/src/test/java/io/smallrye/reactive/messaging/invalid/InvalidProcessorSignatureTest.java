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
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class InvalidProcessorSignatureTest extends WeldTestBaseWithoutTails {

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
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);
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
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);
    }

    @Test
    public void testPayloadToPayloads() {
        addBeanClass(Tails.class);
        addBeanClass(PayloadToPayloads.class);
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);
    }

    @Test
    public void testPayloadToMessages() {
        addBeanClass(Tails.class);
        addBeanClass(PayloadToMessages.class);
        assertThatThrownBy(this::initialize)
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testMessageToMessages() {
        addBeanClass(Tails.class);
        addBeanClass(MessageToMessages.class);
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);
    }

    @Test
    public void testMessageToPayloads() {
        addBeanClass(Tails.class);
        addBeanClass(MessageToPayloads.class);
        assertThatThrownBy(this::initialize)
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testPayloadToPayloadsBuilder() {
        addBeanClass(Tails.class);
        addBeanClass(PayloadToPayloadsBuilder.class);
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);
    }

    @Test
    public void testPayloadToMessagesBuilder() {
        addBeanClass(Tails.class);
        addBeanClass(PayloadToMessagesBuilder.class);
        assertThatThrownBy(this::initialize)
                .isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testMessageToMessagesBuilder() {
        addBeanClass(Tails.class);
        addBeanClass(MessageToMessagesBuilder.class);
        this.initialize();

        Tails bean = get(Tails.class);
        await().until(() -> bean.list().size() == 3);
    }

    @Test
    public void testMessageToPayloadsBuilder() {
        addBeanClass(Tails.class);
        addBeanClass(MessageToPayloadsBuilder.class);
        assertThatThrownBy(this::initialize)
                .isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class PayloadToPayload {
        @Incoming("i")
        @Outgoing("o")
        public String consume(String i) {
            return i;
        }
    }

    @ApplicationScoped
    public static class PayloadToMessage {
        @Incoming("i")
        @Outgoing("o")
        public Message<String> consume(String i) {
            return Message.of(i);
        }
    }

    @ApplicationScoped
    public static class MessageToMessage {
        @Incoming("i")
        @Outgoing("o")
        public Message<String> consume(Message<String> i) {
            return i.withPayload(i.getPayload().toLowerCase());
        }
    }

    @ApplicationScoped
    public static class MessageToPayload {
        @Incoming("i")
        @Outgoing("o")
        public String consume(Message<String> i) {
            return i.getPayload();
        }
    }

    @ApplicationScoped
    public static class PayloadToPayloads {
        @Incoming("i")
        @Outgoing("o")
        public Publisher<String> consume(String i) {
            return Multi.createFrom().item(i);
        }
    }

    @ApplicationScoped
    public static class PayloadToPayloadsBuilder {
        @Incoming("i")
        @Outgoing("o")
        public PublisherBuilder<String> consume(String i) {
            return ReactiveStreams.of(i);
        }
    }

    @ApplicationScoped
    public static class PayloadToMessages {
        @Incoming("i")
        @Outgoing("o")
        public Publisher<Message<String>> consume(String i) {
            return Multi.createFrom().item(Message.of(i));
        }
    }

    @ApplicationScoped
    public static class PayloadToMessagesBuilder {
        @Incoming("i")
        @Outgoing("o")
        public PublisherBuilder<Message<String>> consume(String i) {
            return ReactiveStreams.of(Message.of(i));
        }
    }

    @ApplicationScoped
    public static class MessageToMessages {
        @Incoming("i")
        @Outgoing("o")
        public Publisher<Message<String>> consume(Message<String> i) {
            return Multi.createFrom().item(i.withPayload(i.getPayload().toLowerCase()));
        }
    }

    @ApplicationScoped
    public static class MessageToMessagesBuilder {
        @Incoming("i")
        @Outgoing("o")
        public PublisherBuilder<Message<String>> consume(Message<String> i) {
            return ReactiveStreams.of(i.withPayload(i.getPayload().toLowerCase()));
        }
    }

    @ApplicationScoped
    public static class MessageToPayloads {
        @Incoming("i")
        @Outgoing("o")
        public Publisher<String> consume(Message<String> i) {
            return Multi.createFrom().item(i.getPayload());
        }
    }

    @ApplicationScoped
    public static class MessageToPayloadsBuilder {
        @Incoming("i")
        @Outgoing("o")
        public PublisherBuilder<String> consume(Message<String> i) {
            return ReactiveStreams.of(i.getPayload());
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
