package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ChannelAutoBroadcastTest extends WeldTestBaseWithoutTails {

    @Test
    public void testPayloadBroadcasting() {
        addBeanClass(PayloadConsumer.class, Producer.class);
        initialize();
        Producer producer = get(Producer.class);
        PayloadConsumer consumer = get(PayloadConsumer.class);
        List<Integer> messages1 = new ArrayList<>();
        Cancellable cancellable = consumer.get()
                .subscribe().with(messages1::add);

        producer.emitter().emit(1);
        assertThat(messages1).containsExactly(1);
        assertThat(producer.acks()).isEqualTo(1);

        List<Integer> messages2 = new ArrayList<>();
        Cancellable cancellable2 = consumer.get()
                .subscribe().with(messages2::add);

        producer.emitter().emit(2);
        assertThat(messages1).containsExactly(1, 2);
        assertThat(messages2).containsExactly(2);
        assertThat(producer.acks()).isEqualTo(2);

        producer.emitter().emit(3).emit(4).emit(5);
        assertThat(messages1).containsExactly(1, 2, 3, 4, 5);
        assertThat(messages2).containsExactly(2, 3, 4, 5);
        assertThat(producer.acks()).isEqualTo(5);

        cancellable.cancel();

        producer.emitter().emit(6).emit(7);
        assertThat(messages1).containsExactly(1, 2, 3, 4, 5);
        assertThat(messages2).containsExactly(2, 3, 4, 5, 6, 7);
        assertThat(producer.acks()).isEqualTo(7);

        cancellable2.cancel();

        producer.emitter().emit(8);
        assertThat(messages1).containsExactly(1, 2, 3, 4, 5);
        assertThat(messages2).containsExactly(2, 3, 4, 5, 6, 7);
        assertThat(producer.acks()).isEqualTo(8); // Acknowledging even without consumers
    }

    @ApplicationScoped
    public static class Producer {

        int acks = 0;
        private MultiEmitter<? super Integer> emitter;

        @Outgoing("foo")
        public Multi<Message<Integer>> produce() {
            return Multi.createFrom().<Integer> emitter(e -> emitter = e)
                    .onItem().transform(i -> Message.of(i, () -> {
                        acks++;
                        return CompletableFuture.completedFuture(null);
                    }));
        }

        public int acks() {
            return acks;
        }

        public MultiEmitter<? super Integer> emitter() {
            return emitter;
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
