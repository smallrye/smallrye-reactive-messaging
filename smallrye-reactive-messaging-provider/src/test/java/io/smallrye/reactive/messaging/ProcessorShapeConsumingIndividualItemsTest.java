package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemsAndProducingItems;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemsAndProducingMessages;
import io.smallrye.reactive.messaging.beans.BeanConsumingMessagesAndProducingItems;
import io.smallrye.reactive.messaging.beans.BeanConsumingMessagesAndProducingMessages;

public class ProcessorShapeConsumingIndividualItemsTest extends WeldTestBase {

    private static final List<String> LIST = Multi.createFrom().range(1, 11).map(i -> Integer.toString(i))
            .collect().asList()
            .await().indefinitely();

    @Test
    public void testBeanConsumingMessagesAndProducingMessages() {
        addBeanClass(BeanConsumingMessagesAndProducingMessages.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

    @Test
    public void testBeanConsumingMessagesAndProducingItems() {
        addBeanClass(BeanConsumingMessagesAndProducingItems.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

    @Test
    public void testBeanConsumingItemsAndProducingMessages() {
        addBeanClass(BeanConsumingItemsAndProducingMessages.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

    @Test
    public void testBeanConsumingItemsAndProducingItems() {
        addBeanClass(BeanConsumingItemsAndProducingItems.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

}
