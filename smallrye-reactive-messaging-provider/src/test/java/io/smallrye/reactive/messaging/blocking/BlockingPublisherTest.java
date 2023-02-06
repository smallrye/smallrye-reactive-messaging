package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.PublisherShapeTest;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.blocking.beans.BeanReturningMessages;
import io.smallrye.reactive.messaging.blocking.beans.BeanReturningPayloads;

public class BlockingPublisherTest extends WeldTestBaseWithoutTails {

    @Test
    public void testBlockingWhenProducingPayload() {
        addBeanClass(BeanReturningPayloads.class);
        addBeanClass(PublisherShapeTest.InfiniteSubscriber.class);
        initialize();

        List<Flow.Publisher<? extends Message<?>>> producer = registry(container).getPublishers("infinite-producer");
        assertThat(producer).isNotEmpty();
        PublisherShapeTest.InfiniteSubscriber subscriber = get(PublisherShapeTest.InfiniteSubscriber.class);
        await().until(() -> subscriber.list().size() == 4);
        assertThat(subscriber.list()).containsExactly(1, 2, 3, 4);

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
        addBeanClass(PublisherShapeTest.InfiniteSubscriber.class);
        initialize();

        List<Flow.Publisher<? extends Message<?>>> producer = registry(container).getPublishers("infinite-producer");
        assertThat(producer).isNotEmpty();
        PublisherShapeTest.InfiniteSubscriber subscriber = get(PublisherShapeTest.InfiniteSubscriber.class);
        await().until(() -> subscriber.list().size() == 4);
        assertThat(subscriber.list()).containsExactly(1, 2, 3, 4);

        BeanReturningMessages bean = container.getBeanManager().createInstance().select(BeanReturningMessages.class).get();

        List<String> threadNames = bean.threads().stream().distinct().collect(Collectors.toList());
        assertThat(threadNames.contains(Thread.currentThread().getName())).isFalse();
        for (String name : threadNames) {
            assertThat(name.startsWith("vert.x-worker-thread-")).isTrue();
        }
    }
}
