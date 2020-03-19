package io.smallrye.reactive.messaging.blocking;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;

public class InvalidBlockingTest extends WeldTestBaseWithoutTails {
    @Test(expected = DeploymentException.class)
    public void testBlockingWithNoIncomingOutgoingFails() {
        addBeanClass(InvalidBean.class);
        initialize();
    }

    @Test(expected = DeploymentException.class)
    public void testBlockingWithInvalidWorkerPool1() {
        addBeanClass(ProduceIn.class);
        addBeanClass(EmptyWorkerPoolBean.class);
        initialize();
    }

    @Test(expected = DeploymentException.class)
    public void testBlockingWithInvalidWorkerPool2() {
        addBeanClass(ProduceIn.class);
        addBeanClass(SpacesWorkerPoolBean.class);
        initialize();
    }

    @Test(expected = DeploymentException.class)
    public void testBlockingWithUnconfiguredWorkerPool() {
        addBeanClass(ProduceIn.class);
        addBeanClass(BlockingWorkerPoolBean.class);
        initialize();
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
