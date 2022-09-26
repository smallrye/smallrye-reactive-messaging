package io.smallrye.reactive.messaging.decorator;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.MyCollector;
import io.smallrye.reactive.messaging.WeldTestBase;

public class DecoratorTest extends WeldTestBase {

    @Test
    public void testDecorator() {
        addBeanClass(AppendingDecorator.class, SimpleProducerBean.class);
        initialize();

        MyCollector collector = container.select(MyCollector.class).get();

        // Expect the values in the stream to have "-sink" appended by the decorator
        List<String> expected = SimpleProducerBean.TEST_STRINGS.stream()
                .map((s) -> s + "-sink")
                .collect(Collectors.toList());

        await().until(collector::payloads, hasSize(expected.size()));
        assertEquals(expected, collector.payloads());
    }

    @Test
    void testDeprecatedPublisherDecorator() {
        addBeanClass(AppendingDeprecatedDecorator.class, SimpleProducerBean.class);
        initialize();

        MyCollector collector = container.select(MyCollector.class).get();

        // Expect the values in the stream to have "-sink" appended by the decorator
        List<String> expected = SimpleProducerBean.TEST_STRINGS.stream()
                .map((s) -> s + "-sink")
                .collect(Collectors.toList());

        await().until(collector::payloads, hasSize(expected.size()));
        assertEquals(expected, collector.payloads());
    }

    @Test
    public void testMultiStage() {
        addBeanClass(AppendingDecorator.class, MultiStageBean.class);
        initialize();

        MyCollector collector = container.select(MyCollector.class).get();

        // Expect the values in the stream to be emitted twice and have the name of each channel appended by the decorator
        List<String> expected = MultiStageBean.TEST_STRINGS.stream()
                .flatMap((s) -> Stream.of(s, s))
                .map((s) -> s + "-A-B-sink")
                .collect(Collectors.toList());

        await().until(() -> collector.payloads(), hasSize(expected.size()));
        assertEquals(expected, collector.payloads());
    }

    @Test
    public void testMultiDecorator() {
        addBeanClass(AppendingDecorator.class, CountingDecorator.class, MultiStageBean.class);
        initialize();

        MyCollector collector = container.select(MyCollector.class).get();

        // Expect the values in the stream to be emitted twice and have the name of each channel appended by the decorator
        List<String> expected = MultiStageBean.TEST_STRINGS.stream()
                .flatMap((s) -> Stream.of(s, s))
                .map((s) -> s + "-A-B-sink")
                .collect(Collectors.toList());

        await().until(() -> collector.payloads(), hasSize(expected.size()));
        assertEquals(expected, collector.payloads());

        CountingDecorator countingDecorator = container.select(CountingDecorator.class).get();

        // Each message goes through A, is replicated and then goes through B and sink
        // Therefore each input message results in the counting decorator being incremented
        // five times
        assertEquals(MultiStageBean.TEST_STRINGS.size() * 5, countingDecorator.getMessageCount());
    }
}
