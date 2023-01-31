package io.smallrye.reactive.messaging.merge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.reactivex.processors.UnicastProcessor;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.providers.wiring.TooManyUpstreamCandidatesException;

/**
 * Checks that the deployment fails when a subscriber has 2 many potential publishers and does not use the
 * {@link io.smallrye.reactive.messaging.annotations.Merge} annotation.
 */
public class InvalidBindingTest extends WeldTestBaseWithoutTails {

    @Test
    public void testMissingMerge() {
        addBeanClass(MySource1Bean.class);
        addBeanClass(MySink1Bean.class);
        addBeanClass(MySource2Bean.class);

        try {
            initialize();
            fail("Invalid weaving not detected");
        } catch (DeploymentException e) {
            assertThat(e.getCause())
                    .hasStackTraceContaining(TooManyUpstreamCandidatesException.class.getName())
                    .hasStackTraceContaining("'source'")
                    .hasStackTraceContaining("#sink")
                    .hasStackTraceContaining("found 2");
        }
    }

    @Test
    public void testWithMerge() {
        addBeanClass(MySource1Bean.class);
        addBeanClass(MySinkWithMerge.class);
        addBeanClass(MySource2Bean.class);

        initialize();

        MySinkWithMerge sink = container.getBeanManager().createInstance().select(MySinkWithMerge.class).get();
        assertThat(sink.list().size()).isEqualTo(20);
    }

    @Test
    public void testMissingBroadcast() {
        addBeanClass(MySink2Bean.class);
        addBeanClass(MySink1Bean.class);
        addBeanClass(MyUnicastSourceBean.class);

        try {
            initialize();
            fail("Invalid weaving not detected");
        } catch (DeploymentException e) {
            e.getCause().printStackTrace();
            assertThat(e.getCause())
                    .isInstanceOf(DeploymentException.class)
                    .hasStackTraceContaining("TooManyDownstream");
        }
    }

    @Test
    public void testWithBroadcast() {
        addBeanClass(MySink2Bean.class);
        addBeanClass(MySink1Bean.class);
        addBeanClass(MyUnicastSourceBeanWithBroadcast.class);

        initialize();

        MySink2Bean sink2Bean = container.getBeanManager().createInstance().select(MySink2Bean.class).get();
        MySink1Bean sink1Bean = container.getBeanManager().createInstance().select(MySink1Bean.class).get();
        assertThat(sink2Bean.list()).containsExactly("a", "b");
        assertThat(sink1Bean.list()).containsExactly("a", "b");
    }

    @ApplicationScoped
    public static class MySource1Bean {

        @Outgoing("source")
        public Multi<String> foo() {
            return Multi.createFrom().range(0, 10).map(i -> Integer.toString(i));
        }

    }

    @ApplicationScoped
    public static class MyUnicastSourceBean {

        @Outgoing("source")
        public Publisher<String> foo() {
            return ReactiveStreams.of("a", "b").via(UnicastProcessor.create()).buildRs();
        }

    }

    @ApplicationScoped
    public static class MyUnicastSourceBeanWithBroadcast {

        @Outgoing("source")
        @Broadcast(2)
        public Publisher<String> foo() {
            return ReactiveStreams.of("a", "b").via(UnicastProcessor.create()).buildRs();
        }

    }

    @ApplicationScoped
    public static class MySink1Bean {

        List<String> list = new ArrayList<>();

        @Incoming("source")
        // No merge on purpose
        public void sink(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MySinkWithMerge {

        List<String> list = new ArrayList<>();

        @Incoming("source")
        @Merge
        public void sink(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MySink2Bean {

        List<String> list = new ArrayList<>();

        @Incoming("source")
        public void sink(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MySource2Bean {

        @Outgoing("source")
        public Multi<String> foo() {
            return Multi.createFrom().range(0, 10).map(i -> Integer.toString(i));
        }

    }

}
