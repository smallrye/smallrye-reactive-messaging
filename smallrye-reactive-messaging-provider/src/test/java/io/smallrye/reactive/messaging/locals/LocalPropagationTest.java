package io.smallrye.reactive.messaging.locals;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;
import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.*;
import org.reactivestreams.Publisher;

import io.smallrye.common.vertx.ContextLocals;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.mutiny.core.Context;

public class LocalPropagationTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    void setup() {
        installConfig("src/test/resources/config/locals.properties");
    }

    @AfterAll
    static void cleanup() {
        releaseConfig();
    }

    @RepeatedTest(30)
    public void testLinearPipeline() {
        addBeanClass(ConnectorEmittingOnContext.class);
        addBeanClass(LinearPipeline.class);
        initialize();

        LinearPipeline bean = get(LinearPipeline.class);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @RepeatedTest(30)
    public void testPipelineWithABlockingStage() {
        addBeanClass(ConnectorEmittingOnContext.class);
        addBeanClass(PipelineWithABlockingStage.class);
        initialize();

        PipelineWithABlockingStage bean = get(PipelineWithABlockingStage.class);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @RepeatedTest(30)
    public void testPipelineWithAnAsyncStage() {
        addBeanClass(ConnectorEmittingOnContext.class);
        addBeanClass(PipelineWithAnAsyncStage.class);
        initialize();

        PipelineWithAnAsyncStage bean = get(PipelineWithAnAsyncStage.class);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);

    }

    @RepeatedTest(30)
    public void testPipelineWithAnUnorderedBlockingStage() {
        addBeanClass(ConnectorEmittingOnContext.class);
        addBeanClass(PipelineWithAnUnorderedBlockingStage.class);
        initialize();

        PipelineWithAnUnorderedBlockingStage bean = get(PipelineWithAnUnorderedBlockingStage.class);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactlyInAnyOrder(2, 3, 4, 5, 6);

    }

    @RepeatedTest(30)
    public void testPipelineWithMultipleBlockingStages() {
        addBeanClass(ConnectorEmittingOnContext.class);
        addBeanClass(PipelineWithMultipleBlockingStages.class);
        initialize();

        PipelineWithMultipleBlockingStages bean = get(PipelineWithMultipleBlockingStages.class);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactlyInAnyOrder(2, 3, 4, 5, 6);
    }

    @RepeatedTest(30)
    public void testPipelineWithBroadcastAndMerge() {
        addBeanClass(ConnectorEmittingOnContext.class);
        addBeanClass(PipelineWithBroadcastAndMerge.class);
        initialize();

        PipelineWithBroadcastAndMerge bean = get(PipelineWithBroadcastAndMerge.class);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 10);
        assertThat(bean.getResults()).hasSize(10).contains(2, 3, 4, 5, 6);
    }

    @RepeatedTest(30)
    public void testLinearPipelineWithAckOnCustomThread() {
        installConfig("src/test/resources/config/locals.properties");
        addBeanClass(ConnectorEmittingOnContext.class);
        addBeanClass(LinearPipelineWithAckOnCustomThread.class);
        initialize();

        LinearPipelineWithAckOnCustomThread bean = get(LinearPipelineWithAckOnCustomThread.class);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);

    }

    @RepeatedTest(30)
    public void testLinearPipelineWithEmitter() {
        releaseConfig();
        addBeanClass(EmitterOnContext.class);
        addBeanClass(LinearPipeline.class);
        initialize();

        LinearPipeline bean = get(LinearPipeline.class);
        EmitterOnContext emitter = get(EmitterOnContext.class);

        emitter.emitMessages(Arrays.asList(1, 2, 3, 4, 5));

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @RepeatedTest(30)
    public void testLinearPipelineWithMutinyEmitter() {
        releaseConfig();
        addBeanClass(MutinyEmitterOnContext.class);
        addBeanClass(LinearPipeline.class);
        initialize();

        LinearPipeline bean = get(LinearPipeline.class);
        MutinyEmitterOnContext emitter = get(MutinyEmitterOnContext.class);

        emitter.emitMessages(Arrays.asList(1, 2, 3, 4, 5)).subscribeAsCompletionStage();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Connector("connector-emitting-on-context")
    public static class ConnectorEmittingOnContext implements InboundConnector {

        @Inject
        ExecutionHolder executionHolder;

        @Override
        public Publisher<? extends Message<?>> getPublisher(Config config) {
            Context context = executionHolder.vertx().getOrCreateContext();
            return Multi.createFrom().items(1, 2, 3, 4, 5)
                    .onItem()
                    .transformToUniAndConcatenate(i -> Uni.createFrom().emitter(e -> context.runOnContext(() -> e.complete(i))))
                    .map(Message::of)
                    .map(ContextAwareMessage::withContextMetadata);
        }
    }

    @ApplicationScoped
    public static class LinearPipeline {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();
            ContextLocals.put("uuid", value);
            ContextLocals.put("input", input.getPayload());

            return input.withPayload(input.getPayload() + 1);
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Integer handle(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            assertThat(uuids.add(uuid)).isTrue();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class LinearPipelineWithAckOnCustomThread {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        private final Executor executor = Executors.newFixedThreadPool(4);

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();
            assertThat((String) ContextLocals.get("uuid", null)).isNull();
            ContextLocals.put("uuid", value);
            ContextLocals.put("input", input.getPayload());

            return input.withPayload(input.getPayload() + 1)
                    .withAck(() -> {
                        CompletableFuture<Void> cf = new CompletableFuture<>();
                        executor.execute(() -> cf.complete(null));
                        return cf;
                    });
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Integer handle(int payload) {
            try {
                String uuid = ContextLocals.get("uuid", null);
                assertThat(uuid).isNotNull();

                assertThat(uuids.add(uuid)).isTrue();

                int p = ContextLocals.get("input", null);
                assertThat(p + 1).isEqualTo(payload);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            try {
                String uuid = ContextLocals.get("uuid", null);
                assertThat(uuid).isNotNull();

                int p = ContextLocals.get("input", null);
                assertThat(p + 1).isEqualTo(payload);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithABlockingStage {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();
            assertThat((String) ContextLocals.get("uuid", null)).isNull();
            ContextLocals.put("uuid", value);
            ContextLocals.put("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        @Incoming("process")
        @Outgoing("after-process")
        @Blocking
        public Integer handle(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            assertThat(uuids.add(uuid)).isTrue();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithAnAsyncStage {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();
            assertThat((String) ContextLocals.get("uuid", null)).isNull();
            ContextLocals.put("uuid", value);
            ContextLocals.put("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Uni<Integer> handle(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            assertThat(uuids.add(uuid)).isTrue();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return Uni.createFrom().item(payload).emitOn(Infrastructure.getDefaultExecutor());
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithAnUnorderedBlockingStage {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();
            assertThat((String) ContextLocals.get("uuid", null)).isNull();
            ContextLocals.put("uuid", value);
            ContextLocals.put("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        private final Random random = new Random();

        @Incoming("process")
        @Outgoing("after-process")
        @Blocking(ordered = false)
        public Integer handle(int payload) throws InterruptedException {
            Thread.sleep(random.nextInt(10));
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();
            assertThat(uuids.add(uuid)).isTrue();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithMultipleBlockingStages {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> uuids = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();
            assertThat((String) ContextLocals.get("uuid", null)).isNull();
            ContextLocals.put("uuid", value);
            ContextLocals.put("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        private final Random random = new Random();

        @Incoming("process")
        @Outgoing("second-blocking")
        @Blocking(ordered = false)
        public Integer handle(int payload) throws InterruptedException {
            Thread.sleep(random.nextInt(10));
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();
            assertThat(uuids.add(uuid)).isTrue();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("second-blocking")
        @Outgoing("after-process")
        @Blocking
        public Integer handle2(int payload) throws InterruptedException {
            Thread.sleep(random.nextInt(10));
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PipelineWithBroadcastAndMerge {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Set<String> branch1 = new ConcurrentHashSet<>();
        private final Set<String> branch2 = new ConcurrentHashSet<>();

        @Incoming("data")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        @Broadcast(2)
        public Message<Integer> process(Message<Integer> input) {
            String value = UUID.randomUUID().toString();
            assertThat((String) ContextLocals.get("uuid", null)).isNull();
            ContextLocals.put("uuid", value);
            ContextLocals.put("input", input.getPayload());

            assertThat(input.getMetadata(LocalContextMetadata.class)).isPresent();

            return input.withPayload(input.getPayload() + 1);
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Integer branch1(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();
            assertThat(branch1.add(uuid)).isTrue();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Integer branch2(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();
            assertThat(branch2.add(uuid)).isTrue();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        @Merge
        public Integer afterProcess(int payload) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(payload);
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            String uuid = ContextLocals.get("uuid", null);
            assertThat(uuid).isNotNull();

            int p = ContextLocals.get("input", null);
            assertThat(p + 1).isEqualTo(val);
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class EmitterOnContext {

        @Inject
        ExecutionHolder executionHolder;

        @Inject
        @Channel("data")
        Emitter<Integer> emitter;

        public void emitMessages(List<Integer> payloads) {
            Context context = executionHolder.vertx().getOrCreateContext();
            context.runOnContext(() -> payloads.forEach(p -> emitter.send(p)));
        }
    }

    @ApplicationScoped
    public static class MutinyEmitterOnContext {

        @Inject
        ExecutionHolder executionHolder;

        @Inject
        @Channel("data")
        MutinyEmitter<Integer> emitter;

        public Uni<Void> emitMessages(List<Integer> payloads) {
            Context context = executionHolder.vertx().getOrCreateContext();
            return Multi.createFrom().iterable(payloads)
                    .onItem()
                    .transformToUniAndConcatenate(p -> emitter.send(p).runSubscriptionOn(context::runOnContext))
                    .toUni().replaceWithVoid();
        }
    }

}
