package io.smallrye.reactive.messaging.decorator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.MyCollector;
import io.smallrye.reactive.messaging.WeldTestBase;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.vertx.mutiny.core.Context;

public class TracingDecoratorTest extends WeldTestBase {

    public static final List<String> TEST_STRINGS = Arrays.asList("foo", "bar", "baz");

    @Test
    public void testContextAccessOnSubscriberDecoratorWithOutgoing() {
        addBeanClass(TracingContextDecorator.class, SimpleVertxProducerBean.class);
        initialize();

        MyCollector collector = container.select(MyCollector.class).get();

        await().until(collector::payloads, hasSize(TEST_STRINGS.size()));
        assertEquals(TEST_STRINGS, collector.payloads());
        assertThat(collector.messages()).allSatisfy(m -> {
            assertThat(m.getMetadata(TracingContextDecorator.Trace.class)).contains(TracingContextDecorator.trace);
        });
    }

    @Test
    void testContextAccessOnSubscriberDecoratorWithEmitter() {
        addBeanClass(TracingContextDecorator.class, SimpleVertxEmitterBean.class);
        initialize();
        SimpleVertxEmitterBean emitterBean = get(SimpleVertxEmitterBean.class);
        emitterBean.emitOnVertx();

        MyCollector collector = container.select(MyCollector.class).get();

        await().until(collector::payloads, hasSize(TEST_STRINGS.size()));
        assertEquals(TEST_STRINGS, collector.payloads());
        assertThat(collector.messages()).allSatisfy(m -> {
            assertThat(m.getMetadata(TracingContextDecorator.Trace.class)).contains(TracingContextDecorator.trace);
        });
    }

    @Test
    void testContextAccessOnSubscriberDecoratorWithMutinyEmitter() {
        addBeanClass(TracingContextDecorator.class, SimpleVertxMutinyEmitterBean.class);
        initialize();
        SimpleVertxMutinyEmitterBean emitterBean = get(SimpleVertxMutinyEmitterBean.class);
        emitterBean.emitOnVertx();

        MyCollector collector = container.select(MyCollector.class).get();

        await().until(collector::payloads, hasSize(TEST_STRINGS.size()));
        assertEquals(TEST_STRINGS, collector.payloads());
        assertThat(collector.messages()).allSatisfy(m -> {
            assertThat(m.getMetadata(TracingContextDecorator.Trace.class)).contains(TracingContextDecorator.trace);
        });
    }

    @ApplicationScoped
    public static class SimpleVertxProducerBean {

        @Inject
        ExecutionHolder executionHolder;

        @Outgoing("sink")
        public Multi<Message<?>> createStrings() {
            Context context = executionHolder.vertx().getOrCreateContext();
            return Multi.createFrom().iterable(TEST_STRINGS)
                    .onItem()
                    .transformToUniAndConcatenate(i -> Uni.createFrom().emitter(e -> context.runOnContext(() -> e.complete(i))))
                    .map(ContextAwareMessage::of);
        }

    }

    @ApplicationScoped
    public static class SimpleVertxEmitterBean {

        @Inject
        ExecutionHolder executionHolder;

        @Inject
        @Channel("sink")
        Emitter<String> emitter;

        void emitOnVertx() {
            Context context = executionHolder.vertx().getOrCreateContext();
            for (String s : TEST_STRINGS) {
                context.runOnContext(() -> emitter.send(s));
            }
        }

    }

    @ApplicationScoped
    public static class SimpleVertxMutinyEmitterBean {

        @Inject
        ExecutionHolder executionHolder;

        @Inject
        @Channel("sink")
        MutinyEmitter<String> emitter;

        void emitOnVertx() {
            Context context = executionHolder.vertx().getOrCreateContext();
            for (String s : TEST_STRINGS) {
                context.runOnContext(() -> emitter.send(s).subscribeAsCompletionStage());
            }
        }

    }

}
