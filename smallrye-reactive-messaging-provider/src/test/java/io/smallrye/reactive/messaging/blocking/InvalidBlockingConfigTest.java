package io.smallrye.reactive.messaging.blocking;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

public class InvalidBlockingConfigTest extends WeldTestBaseWithoutTails {
    @Test
    public void testBlockingWithNoIncomingOutgoingFails() {
        addBeanClass(InvalidBean.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBlockingWithInvalidWorkerPool1() {
        addBeanClass(ProduceIn.class);
        addBeanClass(EmptyWorkerPoolBean.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBlockingWithInvalidWorkerPool2() {
        addBeanClass(ProduceIn.class);
        addBeanClass(SpacesWorkerPoolBean.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testBlockingWithUnconfiguredWorkerPool() {
        addBeanClass(ProduceIn.class);
        addBeanClass(BlockingWorkerPoolBean.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class ProduceIn {
        @Outgoing("in")
        public Publisher<String> produce() {
            return Multi.createFrom().items("a", "b", "c");
        }
    }

    @ApplicationScoped
    public static class InvalidBean {

        @Blocking
        public void consume(String s) {
            // Do nothing
        }
    }

    @ApplicationScoped
    public static class SpacesWorkerPoolBean {

        @Incoming("in")
        @Blocking(" ")
        public void consume(String s) {
            // Do nothing
        }
    }

    @ApplicationScoped
    public static class EmptyWorkerPoolBean {

        @Incoming("in")
        @Blocking("")
        public void consume(String s) {
            // Do nothing
        }
    }

    @ApplicationScoped
    public static class BlockingWorkerPoolBean {

        @Incoming("in")
        @Blocking("unknown-pool")
        public void consume(String s) {
            // Do nothing
        }
    }
}
