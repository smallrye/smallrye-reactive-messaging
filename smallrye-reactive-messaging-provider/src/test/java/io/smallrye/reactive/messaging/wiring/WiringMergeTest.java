package io.smallrye.reactive.messaging.wiring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.NoSuchElementException;

import javax.enterprise.inject.spi.Bean;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.DefaultMediatorConfiguration;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.extension.ChannelConfiguration;
import io.smallrye.reactive.messaging.extension.EmitterConfiguration;

@SuppressWarnings("rawtypes")
public class WiringMergeTest {

    @Test
    public void testMerge() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", true));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("processWithMerge"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        EmitterConfiguration ec = new EmitterConfiguration("a", false, null, null);
        ChannelConfiguration cc1 = new ChannelConfiguration("b");

        Wiring wiring = new Wiring();
        wiring.prepare(registry, Collections.singletonList(ec), Collections.singletonList(cc1),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getOutbound()).hasSize(1)
                .allSatisfy(o -> assertThat(o.upstreams()).hasSize(1).allSatisfy(u -> assertThat(u.upstreams()).hasSize(2)));
    }

    @Test
    public void testInvalidMergeWithProcessor() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", true));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        EmitterConfiguration ec = new EmitterConfiguration("a", false, null, null);
        ChannelConfiguration cc1 = new ChannelConfiguration("b");

        Wiring wiring = new Wiring();
        wiring.prepare(registry, Collections.singletonList(ec), Collections.singletonList(cc1),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1).allSatisfy(e -> assertThat(e).isInstanceOf(TooManyUpstreams.class));
    }

    @Test
    public void testInvalidMergeWithSubscriber() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", true));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration subscriber = new DefaultMediatorConfiguration(getMethod("consume"), bean);
        subscriber.compute(Collections.singletonList(IncomingLiteral.of("a")), null, null);

        EmitterConfiguration ec = new EmitterConfiguration("a", false, null, null);

        Wiring wiring = new Wiring();
        wiring.prepare(registry, Collections.singletonList(ec), Collections.emptyList(),
                Collections.singletonList(subscriber));
        Graph graph = wiring.resolve();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1).allSatisfy(e -> assertThat(e).isInstanceOf(TooManyUpstreams.class));
    }

    @Test
    public void testInvalidMergeWithOutgoingConnector() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", true));
        when(registry.getOutgoingNames()).thenReturn(Collections.singleton("a"));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        EmitterConfiguration ec = new EmitterConfiguration("a", false, null, null);

        Wiring wiring = new Wiring();
        wiring.prepare(registry, Collections.singletonList(ec), Collections.emptyList(),
                Collections.emptyList());
        Graph graph = wiring.resolve();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1).allSatisfy(e -> assertThat(e).isInstanceOf(TooManyUpstreams.class));
    }

    private Method getMethod(String name) {
        for (Method method : this.getClass().getMethods()) {
            if (method.getName().equals(name)) {
                return method;
            }
        }
        throw new NoSuchElementException("No method " + name);
    }

    public void consume(String ignored) {
        // ...
    }

    public String producer() {
        // ...
        return "hello";
    }

    public String process(String s) {
        return s;
    }

    @Merge
    public String processWithMerge(String s) {
        return s;
    }

}
