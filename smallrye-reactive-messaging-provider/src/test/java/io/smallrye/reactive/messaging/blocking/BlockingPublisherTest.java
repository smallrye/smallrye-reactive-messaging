package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.blocking.beans.BeanReturningMessages;
import io.smallrye.reactive.messaging.blocking.beans.BeanReturningPayloads;

public class BlockingPublisherTest extends WeldTestBaseWithoutTails {

    @Test
    public void testBlockingWhenProducingPayload() {
        addBeanClass(BeanReturningPayloads.class);
        initialize();

        List<PublisherBuilder<? extends Message<?>>> producer = registry(container).getPublishers("infinite-producer");
        assertThat(producer).isNotEmpty();
        List<Integer> list = producer.get(0).map(Message::getPayload)
                .limit(5)
                .map(i -> (Integer) i)
                .toList().run().toCompletableFuture().join();
        assertThat(list).containsExactly(1, 2, 3, 4, 5);

        BeanReturningPayloads bean = container.getBeanManager().createInstance().select(BeanReturningPayloads.class).get();

        List<String> threadNames = bean.threads().stream().distinct().collect(Collectors.toList());
        assertThat(threadNames.contains(Thread.currentThread().getName())).isFalse();
        for (String name : threadNames) {
            assertThat(name.startsWith("vert.x-worker-thread-")).isTrue();
        }
    }

    @Test
    public void testBlockingWhenProducingMessages() {
        addBeanClass(BeanReturningMessages.class);
        initialize();

        List<PublisherBuilder<? extends Message<?>>> producer = registry(container).getPublishers("infinite-producer");
        assertThat(producer).isNotEmpty();
        List<Integer> list = producer.get(0).map(Message::getPayload)
                .limit(5)
                .map(i -> (Integer) i)
                .toList().run().toCompletableFuture().join();
        assertThat(list).containsExactly(1, 2, 3, 4, 5);

        BeanReturningMessages bean = container.getBeanManager().createInstance().select(BeanReturningMessages.class).get();

        List<String> threadNames = bean.threads().stream().distinct().collect(Collectors.toList());
        assertThat(threadNames.contains(Thread.currentThread().getName())).isFalse();
        for (String name : threadNames) {
            assertThat(name.startsWith("vert.x-worker-thread-")).isTrue();
        }
    }
}
