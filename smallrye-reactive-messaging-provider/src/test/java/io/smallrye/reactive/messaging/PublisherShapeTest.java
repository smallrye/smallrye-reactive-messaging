package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.se.SeContainer;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.beans.*;

public class PublisherShapeTest extends WeldTestBaseWithoutTails {

    @Override
    public List<Class<?>> getBeans() {
        return Collections.singletonList(CollectorOnly.class);
    }

    @Test
    public void testBeanProducingMessagesAsCustomPublisher() {
        addBeanClass(BeanProducingMessagesAsCustomPublisher.class);
        initialize();
        CollectorOnly collector = container.select(CollectorOnly.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingMessagesAsMulti() {
        addBeanClass(BeanProducingMessagesAsMulti.class);
        initialize();
        CollectorOnly collector = container.select(CollectorOnly.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingPayloadsAsCustomPublisher() {
        addBeanClass(BeanProducingPayloadAsCustomPublisher.class);
        initialize();
        CollectorOnly collector = container.select(CollectorOnly.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingPayloadsAsMulti() {
        addBeanClass(BeanProducingPayloadAsMulti.class);
        initialize();
        CollectorOnly collector = container.select(CollectorOnly.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingMessagesAsPublisher() {
        addBeanClass(BeanProducingMessagesAsPublisher.class);
        initialize();
        CollectorOnly collector = container.select(CollectorOnly.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingPayloadsAsPublisher() {
        addBeanClass(BeanProducingPayloadAsPublisher.class);
        initialize();
        CollectorOnly collector = container.select(CollectorOnly.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingMessagesAsPublisherBuilder() {
        addBeanClass(BeanProducingMessagesAsPublisherBuilder.class);
        initialize();
        CollectorOnly collector = container.select(CollectorOnly.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testBeanProducingPayloadAsPublisherBuilder() {
        addBeanClass(BeanProducingPayloadAsPublisherBuilder.class);
        initialize();
        CollectorOnly collector = container.select(CollectorOnly.class).get();
        assertThat(collector.payloads()).isEqualTo(EXPECTED);
    }

    @Test
    public void testThatWeCanProducePublisherOfMessages() {
        addBeanClass(MyProducerSink.class);
        addBeanClass(BeanReturningAPublisherBuilderOfItems.class);
        initialize();
        assertThatProducerWasPublished(container);
    }

    @Test
    public void testThatWeCanProducePublisherBuilderOfMessages() {
        addBeanClass(BeanReturningAPublisherBuilderOfMessages.class);
        addBeanClass(MyProducerSink.class);
        initialize();
        assertThatProducerWasPublished(container);
    }

    @Test
    public void testThatWeCanProducePublisherOfItems() {
        addBeanClass(MyProducerSink.class);
        addBeanClass(BeanReturningAPublisherBuilderOfItems.class);
        initialize();
        assertThatProducerWasPublished(container);
    }

    @Test
    public void testThatWeCanProducePublisherBuilderOfItems() {
        addBeanClass(MyProducerSink.class);
        addBeanClass(BeanReturningAPublisherBuilderOfItems.class);
        initialize();
        assertThatProducerWasPublished(container);
    }

    @Test
    public void testThatWeCanProducePayloadDirectly() {
        addBeanClass(BeanReturningPayloads.class);
        addBeanClass(InfiniteSubscriber.class);
        initialize();

        List<Publisher<? extends Message<?>>> producer = registry(container).getPublishers("infinite-producer");
        assertThat(producer).isNotEmpty();
        InfiniteSubscriber subscriber = get(InfiniteSubscriber.class);
        await().until(() -> subscriber.list().size() == 4);
        assertThat(subscriber.list()).containsExactly(1, 2, 3, 4);
    }

    @Test
    public void testThatWeCanProduceMessageDirectly() {
        addBeanClass(BeanReturningMessages.class);
        addBeanClass(InfiniteSubscriber.class);
        initialize();

        List<Publisher<? extends Message<?>>> producer = registry(container).getPublishers("infinite-producer");
        assertThat(producer).isNotEmpty();
        InfiniteSubscriber subscriber = get(InfiniteSubscriber.class);
        await().until(() -> subscriber.list().size() == 4);
        assertThat(subscriber.list()).containsExactly(1, 2, 3, 4);
    }

    @Test
    public void testThatWeCanProduceCompletionStageOfMessageDirectly() {
        addBeanClass(BeanReturningCompletionStageOfMessage.class);
        addBeanClass(InfiniteSubscriber.class);
        initialize();

        List<Publisher<? extends Message<?>>> producer = registry(container).getPublishers("infinite-producer");
        assertThat(producer).isNotEmpty();
        InfiniteSubscriber subscriber = get(InfiniteSubscriber.class);
        await().until(() -> subscriber.list().size() == 4);
        assertThat(subscriber.list()).containsExactly(1, 2, 3, 4);
        container.select(BeanReturningCompletionStageOfMessage.class).get().close();
    }

    @Test
    public void testThatWeCanProduceCompletionStageOfPayloadDirectly() {
        addBeanClass(BeanReturningCompletionStageOfPayload.class);
        addBeanClass(InfiniteSubscriber.class);
        initialize();

        List<Publisher<? extends Message<?>>> producer = registry(container).getPublishers("infinite-producer");
        assertThat(producer).isNotEmpty();

        InfiniteSubscriber subscriber = get(InfiniteSubscriber.class);
        await().until(() -> subscriber.list().size() == 4);
        assertThat(subscriber.list()).containsExactly(1, 2, 3, 4);
        container.select(BeanReturningCompletionStageOfPayload.class).get().close();
    }

    @ApplicationScoped
    public static class MyProducerSink {
        @Incoming("producer")
        void consume(String s) {

        }
    }

    @ApplicationScoped
    public static class InfiniteSubscriber {

        private final List<Integer> list = new ArrayList<>();

        @Incoming("infinite-producer")
        public SubscriberBuilder<Integer, Void> consumeFourItems() {
            return ReactiveStreams
                    .<Integer> builder()
                    .limit(4)
                    .forEach(list::add);
        }

        public List<Integer> list() {
            return list;
        }
    }

    private void assertThatProducerWasPublished(SeContainer container) {
        assertThat(registry(container).getIncomingNames()).contains("producer");
        List<Publisher<? extends Message<?>>> producer = registry(container).getPublishers("producer");
        assertThat(producer).isNotEmpty();
        List<String> list = Multi.createFrom().publisher(producer.get(0)).map(Message::getPayload)
                .map(i -> (String) i)
                .collect().asList()
                .await().indefinitely();
        assertThat(list).containsExactly("a", "b", "c");
    }

}
