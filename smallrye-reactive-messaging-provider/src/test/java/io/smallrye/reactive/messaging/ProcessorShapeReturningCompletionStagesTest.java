package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.beans.BeanConsumingItemAndProducingCompletionStageOfMessage;
import io.smallrye.reactive.messaging.beans.BeanConsumingMessagesAndProducingCompletionStageOfSomething;
import io.smallrye.reactive.messaging.beans.BeanProducingACompletableFuture;
import io.smallrye.reactive.messaging.beans.BeanProducingACompletableFutureOfMessage;
import io.smallrye.reactive.messaging.beans.BeanProducingACompletionStage;
import io.smallrye.reactive.messaging.beans.BeanProducingACompletionStageOfMessage;

public class ProcessorShapeReturningCompletionStagesTest extends WeldTestBase {

    private static final List<String> LIST = Multi.createFrom().range(1, 11).map(i -> Integer.toString(i))
            .collect().asList()
            .await().indefinitely();

    @Test
    public void testBeanProducingACompletionStageOfMessage() {
        addBeanClass(BeanProducingACompletionStageOfMessage.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(() -> collector.payloads().size() == LIST.size());
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

    @Test
    public void testBeanConsumingItemAndProducingACompletionStageOfMessage() {
        addBeanClass(BeanConsumingItemAndProducingCompletionStageOfMessage.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(() -> collector.payloads().size() == LIST.size());
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

    @Test
    public void testBeanProducingACompletionStageOfPayloads() {
        addBeanClass(BeanProducingACompletionStage.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(() -> collector.payloads().size() == LIST.size());
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

    @Test
    public void testBeanConsumingMessageAndProducingACompletionStageOfPayloads() {
        addBeanClass(BeanConsumingMessagesAndProducingCompletionStageOfSomething.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(() -> collector.payloads().size() == LIST.size());
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

    @Test
    public void testBeanProducingACompletableFutureOfMessage() {
        addBeanClass(BeanProducingACompletableFutureOfMessage.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(() -> collector.payloads().size() == LIST.size());
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

    @Test
    public void testBeanProducingACompletableFutureOfPayloads() {
        addBeanClass(BeanProducingACompletableFuture.class);
        initialize();
        MyCollector collector = container.select(MyCollector.class).get();
        await().until(() -> collector.payloads().size() == LIST.size());
        assertThat(collector.payloads()).isEqualTo(LIST);
    }

}
