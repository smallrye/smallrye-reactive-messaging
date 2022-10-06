package io.smallrye.reactive.messaging.locals;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.vertx.mutiny.core.Vertx;

public class LocalPropagationWithoutContextTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    void setup() {
        installConfig("src/test/resources/config/locals-no-context.properties");
    }

    @AfterAll
    static void cleanup() {
        releaseConfig();
    }

    @RepeatedTest(30)
    public void testLinearPipelineNoContext() {
        addBeanClass(ConnectorEmittingDirectly.class);
        addBeanClass(LinearPipelineNoContext.class);
        initialize();

        LinearPipelineNoContext bean = get(LinearPipelineNoContext.class);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Connector("connector-without-context")
    public static class ConnectorEmittingDirectly implements InboundConnector {

        @Override
        public Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().items(1, 2, 3, 4, 5)
                    .map(Message::of)
                    .onItem().transformToUniAndConcatenate(i -> Uni.createFrom().emitter(e -> e.complete(i)));
        }
    }

    @ApplicationScoped
    public static class LinearPipelineNoContext {

        private final List<Integer> list = new CopyOnWriteArrayList<>();

        @Incoming("data-no-context")
        @Outgoing("process")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> input) {
            assertThat(Vertx.currentContext()).isNull();
            return input.withPayload(input.getPayload() + 1);
        }

        @Incoming("process")
        @Outgoing("after-process")
        public Integer handle(int payload) {
            assertThat(Vertx.currentContext()).isNull();
            return payload;
        }

        @Incoming("after-process")
        @Outgoing("sink")
        public Integer afterProcess(int payload) {
            assertThat(Vertx.currentContext()).isNull();
            return payload;
        }

        @Incoming("sink")
        public void sink(int val) {
            assertThat(Vertx.currentContext()).isNull();
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }
    }

}
