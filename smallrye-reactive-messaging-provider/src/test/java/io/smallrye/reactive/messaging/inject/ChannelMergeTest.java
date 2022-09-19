package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Broadcast;

public class ChannelMergeTest extends WeldTestBaseWithoutTails {

    @Test
    public void testMissing() {
        addBeanClass(ChannelConsumer.class);
        initialize();
        ChannelConsumer consumer = get(ChannelConsumer.class);
        assertThatThrownBy(() -> consumer.getMessages().collect().asList().await().indefinitely())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SRMSG00018");
        assertThatThrownBy(() -> consumer.getPayloads().collect().asList().await().indefinitely())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SRMSG00018");
    }

    @Test
    public void testOne() {
        addBeanClass(ChannelConsumer.class, Producer1.class);
        initialize();
        ChannelConsumer consumer = get(ChannelConsumer.class);
        List<Integer> payloads = new ArrayList<>();
        List<Message<Integer>> messages = new ArrayList<>();
        consumer.getPayloads()
                .subscribe().with(payloads::add);
        consumer.getMessages()
                .subscribe().with(messages::add);

        assertThat(messages.stream().map(Message::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(payloads)
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testMerge() {
        addBeanClass(ChannelConsumer.class, Producer1.class, Producer2.class);
        initialize();
        ChannelConsumer consumer = get(ChannelConsumer.class);
        List<Integer> payloads = new ArrayList<>();
        List<Message<Integer>> messages = new ArrayList<>();
        consumer.getPayloads()
                .subscribe().with(payloads::add);
        consumer.getMessages()
                .subscribe().with(messages::add);

        assertThat(messages.stream().map(Message::getPayload).collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(payloads)
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @ApplicationScoped
    public static class Producer1 {
        @Outgoing("foo")
        @Broadcast(2)
        public Multi<Integer> produce() {
            return Multi.createFrom().range(0, 10);
        }
    }

    @ApplicationScoped
    public static class Producer2 {
        @Outgoing("foo")
        @Broadcast(2)
        public Multi<Integer> produce() {
            return Multi.createFrom().range(0, 10);
        }
    }

    @ApplicationScoped
    public static class ChannelConsumer {
        @Inject
        @Channel("foo")
        Multi<Integer> multiOfPayloads;

        @Inject
        @Channel("foo")
        Multi<Message<Integer>> multiOfMessages;

        public Multi<Integer> getPayloads() {
            return multiOfPayloads;
        }

        public Multi<Message<Integer>> getMessages() {
            return multiOfMessages;
        }
    }

}
