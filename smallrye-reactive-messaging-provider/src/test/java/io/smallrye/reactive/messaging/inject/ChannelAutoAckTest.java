package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ChannelAutoAckTest extends WeldTestBaseWithoutTails {

    @Test
    public void testWithMessageAndOneSource() {
        addBeanClass(MessageConsumer.class, Producer1.class);
        initialize();
        MessageConsumer consumer = get(MessageConsumer.class);
        List<Message<Integer>> messages = new ArrayList<>();
        consumer.get()
                .subscribe().with(messages::add);
        assertThat(messages.stream().map(Message::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(get(Producer1.class).acks()).isEqualTo(0);
        messages.forEach(Message::ack);
        assertThat(get(Producer1.class).acks()).isEqualTo(10);
    }

    @Test
    public void testWithPayloadAndOneSource() {
        addBeanClass(PayloadConsumer.class, Producer1.class);
        initialize();
        PayloadConsumer consumer = get(PayloadConsumer.class);
        List<Integer> messages = new ArrayList<>();
        consumer.get()
                .subscribe().with(messages::add);
        assertThat(messages)
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(get(Producer1.class).acks()).isEqualTo(10);
        assertThat(get(Producer1.class).acks()).isEqualTo(10);
    }

    @Test
    public void testWithMessagesAndTwoSources() {
        addBeanClass(MessageConsumer.class, Producer1.class, Producer2.class);
        initialize();
        MessageConsumer consumer = get(MessageConsumer.class);
        List<Message<Integer>> messages = new ArrayList<>();
        consumer.get()
                .subscribe().with(messages::add);
        assertThat(messages.stream().map(Message::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(get(Producer1.class).acks()).isEqualTo(0);
        assertThat(get(Producer2.class).acks()).isEqualTo(0);
        messages.forEach(Message::ack);
        assertThat(get(Producer1.class).acks()).isEqualTo(10);
        assertThat(get(Producer2.class).acks()).isEqualTo(10);
    }

    @Test
    public void testWithPayloadsAndTwoSources() {
        addBeanClass(PayloadConsumer.class, Producer1.class, Producer2.class);
        initialize();
        PayloadConsumer consumer = get(PayloadConsumer.class);
        List<Integer> messages = new ArrayList<>();
        consumer.get()
                .subscribe().with(messages::add);
        assertThat(messages)
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(get(Producer1.class).acks()).isEqualTo(10);
        assertThat(get(Producer2.class).acks()).isEqualTo(10);
    }

    @ApplicationScoped
    public static class Producer1 {

        int acks = 0;

        @Outgoing("foo")
        public Multi<Message<Integer>> produce() {
            return Multi.createFrom().range(0, 10)
                    .onItem().transform(i -> Message.of(i, () -> {
                        acks++;
                        return CompletableFuture.completedFuture(null);
                    }));
        }

        public int acks() {
            return acks;
        }
    }

    @ApplicationScoped
    public static class Producer2 {

        int acks = 0;

        @Outgoing("foo")
        public Multi<Message<Integer>> produce() {
            return Multi.createFrom().range(0, 10)
                    .onItem().transform(i -> Message.of(i, () -> {
                        acks++;
                        return CompletableFuture.completedFuture(null);
                    }));
        }

        public int acks() {
            return acks;
        }
    }

    @ApplicationScoped
    public static class MessageConsumer {

        @Inject
        @Channel("foo")
        Multi<Message<Integer>> multiOfMessages;

        public Multi<Message<Integer>> get() {
            return multiOfMessages;
        }
    }

    @ApplicationScoped
    public static class PayloadConsumer {

        @Inject
        @Channel("foo")
        Multi<Integer> multiOfPayloads;

        public Multi<Integer> get() {
            return multiOfPayloads;
        }
    }

}
