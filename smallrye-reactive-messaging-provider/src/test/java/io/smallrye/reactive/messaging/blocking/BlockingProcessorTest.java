package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MyCollector;
import io.smallrye.reactive.messaging.WeldTestBase;
import io.smallrye.reactive.messaging.blocking.beans.BeanConsumingItemsAndProducingItems;
import io.smallrye.reactive.messaging.blocking.beans.BeanConsumingItemsAndProducingMessages;
import io.smallrye.reactive.messaging.blocking.beans.BeanConsumingMessagesAndProducingItems;
import io.smallrye.reactive.messaging.blocking.beans.BeanConsumingMessagesAndProducingMessages;

public class BlockingProcessorTest extends WeldTestBase {

    private static final List<String> LIST = Multi.createFrom().range(1, 11).map(i -> Integer.toString(i))
            .collect().asList()
            .await().indefinitely();

    @Test
    public void testBeanConsumingMessagesAndProducingMessages() {
        addBeanClass(BeanConsumingMessagesAndProducingMessages.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(collector::hasCompleted);
        assertThat(collector.payloads()).isEqualTo(LIST);

        BeanConsumingMessagesAndProducingMessages bean = container.select(BeanConsumingMessagesAndProducingMessages.class)
                .get();

        List<String> threadNames = bean.threads().stream().distinct().collect(Collectors.toList());
        assertThat(threadNames.contains(Thread.currentThread().getName())).isFalse();
        for (String name : threadNames) {
            assertThat(name.startsWith("vert.x-worker-thread-")).isTrue();
        }
    }

    @Test
    public void testBeanConsumingMessagesAndProducingItems() {
        addBeanClass(BeanConsumingMessagesAndProducingItems.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(collector::hasCompleted);
        assertThat(collector.payloads()).isEqualTo(LIST);

        BeanConsumingMessagesAndProducingItems bean = container.select(BeanConsumingMessagesAndProducingItems.class).get();

        List<String> threadNames = bean.threads().stream().distinct().collect(Collectors.toList());
        assertThat(threadNames.contains(Thread.currentThread().getName())).isFalse();
        for (String name : threadNames) {
            assertThat(name.startsWith("vert.x-worker-thread-")).isTrue();
        }
    }

    @Test
    public void testBeanConsumingItemsAndProducingMessages() {
        addBeanClass(BeanConsumingItemsAndProducingMessages.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(collector::hasCompleted);
        assertThat(collector.payloads()).isEqualTo(LIST);

        BeanConsumingItemsAndProducingMessages bean = container.select(BeanConsumingItemsAndProducingMessages.class).get();

        List<String> threadNames = bean.threads().stream().distinct().collect(Collectors.toList());
        assertThat(threadNames.contains(Thread.currentThread().getName())).isFalse();
        for (String name : threadNames) {
            assertThat(name.startsWith("vert.x-worker-thread-")).isTrue();
        }
    }

    @Test
    public void testBeanConsumingItemsAndProducingItems() {
        addBeanClass(BeanConsumingItemsAndProducingItems.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(collector::hasCompleted);
        assertThat(collector.payloads()).isEqualTo(LIST);

        BeanConsumingItemsAndProducingItems bean = container.select(BeanConsumingItemsAndProducingItems.class).get();

        List<String> threadNames = bean.threads().stream().distinct().collect(Collectors.toList());
        assertThat(threadNames.contains(Thread.currentThread().getName())).isFalse();
        for (String name : threadNames) {
            assertThat(name.startsWith("vert.x-worker-thread-")).isTrue();
        }
    }

}
