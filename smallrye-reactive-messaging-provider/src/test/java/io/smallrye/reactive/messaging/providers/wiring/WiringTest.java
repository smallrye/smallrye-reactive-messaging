package io.smallrye.reactive.messaging.providers.wiring;

import static io.smallrye.reactive.messaging.annotations.EmitterFactoryFor.Literal.EMITTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;

import jakarta.enterprise.inject.spi.Bean;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.providers.DefaultEmitterConfiguration;
import io.smallrye.reactive.messaging.providers.DefaultMediatorConfiguration;
import io.smallrye.reactive.messaging.providers.extension.ChannelConfiguration;

@SuppressWarnings("rawtypes")
class WiringTest {
    @Test
    public void testEmptyGraph() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).isEmpty();
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();
        assertThat(graph.getInbound()).isEmpty();
        assertThat(graph.getOutbound()).isEmpty();
    }

    /**
     * Only use methods (producer, process, consumer)
     * a -> b -> c
     */
    @Test
    public void testProducerProcessorSubscriberChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration subscriber = new DefaultMediatorConfiguration(getMethod("consume"), bean);
        subscriber.compute(Collections.singletonList(IncomingLiteral.of("b")), null, null);
        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(subscriber, processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(3);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.incomings()).containsExactly("b"));
    }

    /**
     * Only use methods (producer, process, consumer)
     * a -> b -> c
     *
     * Enable the strict mode.
     */
    @Test
    public void testProducerProcessorSubscriberChainStrict() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration subscriber = new DefaultMediatorConfiguration(getMethod("consume"), bean);
        subscriber.compute(Collections.singletonList(IncomingLiteral.of("b")), null, null);
        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(true, registry, Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(subscriber, processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(3);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.incomings()).containsExactly("b"));

    }

    /**
     * Only use methods (process, consumer)
     * X -> b -> c
     */
    @Test
    public void testNoProducerProcessorSubscriberChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration subscriber = new DefaultMediatorConfiguration(getMethod("consume"), bean);
        subscriber.compute(Collections.singletonList(IncomingLiteral.of("b")), null, null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(subscriber, processor));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(1);
        assertThat(graph.getUnresolvedComponents()).hasSize(1);
        assertThat(graph.isClosed()).isFalse();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).isEmpty();
        assertThat(graph.getOutbound()).isEmpty();
    }

    /**
     * Only use methods (producer, consumer)
     * a -> X -> c
     */
    @Test
    public void testProducerNoProcessorSubscriberChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration subscriber = new DefaultMediatorConfiguration(getMethod("consume"), bean);
        subscriber.compute(Collections.singletonList(IncomingLiteral.of("b")), null, null);
        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(subscriber, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(1); // Only one resolved component (producer)
        assertThat(graph.getUnresolvedComponents()).hasSize(1);
        assertThat(graph.isClosed()).isFalse();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).isEmpty();
        assertThat(graph.getOutbound()).isEmpty();
    }

    /**
     * Only use methods (producer, process)
     * a -> b -> X
     */
    @Test
    public void testProducerProcessorNoSubscriberChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(2);
        assertThat(graph.getUnresolvedComponents()).hasSize(0);
        assertThat(graph.isClosed()).isFalse();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).isEmpty();
        assertThat(graph.getOutbound()).isEmpty();
    }

    /**
     * Only use methods (producer, process)
     * a -> b -> X
     *
     * Strict mode enabled
     */
    @Test
    public void testProducerProcessorNoSubscriberChainStrict() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(true, registry, Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(2);
        assertThat(graph.getUnresolvedComponents()).hasSize(0);
        assertThat(graph.isClosed()).isFalse();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1).allSatisfy(e -> {
            assertThat(e).isInstanceOf(OpenGraphException.class);
            assertThat(e).hasMessageContaining("b");
        });

        assertThat(graph.getInbound()).isEmpty();
        assertThat(graph.getOutbound()).isEmpty();
    }

    /**
     * Two connectors (inbound, outbound) and a method processing in between.
     * connector -> process -> connector.
     */
    @Test
    public void testConnectorProcessorConnectorChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", false));
        when(registry.getOutgoingChannels()).thenReturn(Collections.singletonMap("b", false));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Collections.emptyList(),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(3);
        assertThat(graph.getUnresolvedComponents()).hasSize(0);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.incomings()).containsExactly("b"));
    }

    /**
     * Two connectors (inbound, outbound) not connected with a processor.
     * connector -> X -> connector.
     */
    @Test
    public void testConnectorNoProcessorConnectorChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", false));
        when(registry.getOutgoingChannels()).thenReturn(Collections.singletonMap("b", false));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(1);
        assertThat(graph.getUnresolvedComponents()).hasSize(1);
        assertThat(graph.isClosed()).isFalse();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(0);
        assertThat(graph.getOutbound()).hasSize(0);
    }

    /**
     * Two connectors (inbound, outbound) connected directly.
     * connector -> connector.
     */
    @Test
    public void testConnectorToConnectorChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", false));
        when(registry.getOutgoingChannels()).thenReturn(Collections.singletonMap("a", false));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(2);
        assertThat(graph.getUnresolvedComponents()).hasSize(0);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.incomings()).containsExactly("a"));
    }

    /**
     * Two connectors (inbound, outbound) connected directly.
     * connector -> connector.
     */
    @Test
    public void testConnectorToConnectorChainWithOutgoingConnectorAbleToMerge() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", false));
        when(registry.getOutgoingChannels()).thenReturn(Collections.singletonMap("a", true));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(2);
        assertThat(graph.getUnresolvedComponents()).hasSize(0);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.incomings()).containsExactly("a"));
    }

    /**
     * Use an emitter, and two methods to process and subscribe
     * emitter -> b -> c
     */
    @Test
    public void testEmitterProcessorSubscriberChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration subscriber = new DefaultMediatorConfiguration(getMethod("consume"), bean);
        subscriber.compute(Collections.singletonList(IncomingLiteral.of("b")), null, null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry,
                Collections.singletonList(new DefaultEmitterConfiguration("a", EMITTER, null, null)),
                Collections.emptyList(),
                Arrays.asList(subscriber, processor));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(3);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.incomings()).containsExactly("b"));
    }

    /**
     * Use an emitter, and the process method.
     * emitter -> b -> X
     */
    @Test
    public void testEmitterProcessorNoSubscriberChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false,
                registry,
                Collections.singletonList(new DefaultEmitterConfiguration("a", EMITTER, null, null)),
                Collections.emptyList(),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(2);
        assertThat(graph.isClosed()).isFalse();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).isEmpty();
        assertThat(graph.getOutbound()).isEmpty();
    }

    /**
     * Use an emitter, and a subscriber method, but no processor
     * emitter -> X -> c
     */
    @Test
    public void testEmitterNoProcessorSubscriberChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration subscriber = new DefaultMediatorConfiguration(getMethod("consume"), bean);
        subscriber.compute(Collections.singletonList(IncomingLiteral.of("b")), null, null);

        Wiring wiring = new Wiring();
        wiring.prepare(false,
                registry,
                Collections.singletonList(new DefaultEmitterConfiguration("a", EMITTER, null, null)),
                Collections.emptyList(),
                Collections.singletonList(subscriber));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(1);
        assertThat(graph.isClosed()).isFalse();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).isEmpty();
        assertThat(graph.getOutbound()).isEmpty();
    }

    /**
     * Use an emitter, and one method to process and an injected channel
     * emitter -> b -> channel
     */
    @Test
    public void testEmitterProcessorChannelChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false,
                registry,
                Collections.singletonList(new DefaultEmitterConfiguration("a", EMITTER, null, null)),
                Collections.singletonList(new ChannelConfiguration("b")),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(3);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.incomings()).containsExactly("b"));
    }

    /**
     * Use an emitter, and an injected channel - no processor
     * emitter -> X -> channel
     */
    @Test
    public void testEmitterNoProcessorChannelChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        Wiring wiring = new Wiring();
        wiring.prepare(false,
                registry,
                Collections.singletonList(new DefaultEmitterConfiguration("a", EMITTER, null, null)),
                Collections.singletonList(new ChannelConfiguration("b")),
                Collections.emptyList());
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(1);
        assertThat(graph.isClosed()).isFalse();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).isEmpty();
        assertThat(graph.getOutbound()).isEmpty();
    }

    /**
     * Use an emitter, directly connected to an injected channel.
     * emitter -> b -> channel
     */
    @Test
    public void testEmitterToChannelChain() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        Wiring wiring = new Wiring();
        wiring.prepare(false,
                registry,
                Collections.singletonList(new DefaultEmitterConfiguration("a", EMITTER, null, null)),
                Collections.singletonList(new ChannelConfiguration("a")),
                Collections.emptyList());
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(2);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.incomings()).containsExactly("a"));
    }

    /**
     * A processor consuming "a" and producing "b" and another processor consuming "b" and producing "a"
     */
    @Test
    public void testDirectCycle() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor1 = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor1.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);
        DefaultMediatorConfiguration processor2 = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor2.compute(Collections.singletonList(IncomingLiteral.of("b")), OutgoingLiteral.of("a"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false,
                registry,
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(processor1, processor2));
        assertThatThrownBy(wiring::resolve).isInstanceOf(CycleException.class);
    }

    /**
     * A processor consuming "a" and producing "b" and another processor consuming "b" and producing "c" and a third
     * processor consuming "c" and producing "a".
     */
    @Test
    public void testCycleWithAComponentInBetween() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor1 = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor1.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);
        DefaultMediatorConfiguration processor2 = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor2.compute(Collections.singletonList(IncomingLiteral.of("b")), OutgoingLiteral.of("c"), null);
        DefaultMediatorConfiguration processor3 = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor3.compute(Collections.singletonList(IncomingLiteral.of("c")), OutgoingLiteral.of("a"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false,
                registry,
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(processor1, processor2, processor3));
        assertThatThrownBy(wiring::resolve).isInstanceOf(CycleException.class);
    }

    /**
     * A connector producing "x".
     * A processor consuming "a and x" and producing "b" and another processor consuming "b" and producing "c" and a third
     * processor consuming "c" and producing "a".
     */
    @Test
    public void testCycleInDownstreams() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("x", false));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor1 = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor1.compute(Arrays.asList(IncomingLiteral.of("x"), IncomingLiteral.of("a")),
                OutgoingLiteral.of("b"), null);
        DefaultMediatorConfiguration processor2 = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor2.compute(Collections.singletonList(IncomingLiteral.of("b")), OutgoingLiteral.of("c"), null);
        DefaultMediatorConfiguration processor3 = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor3.compute(Collections.singletonList(IncomingLiteral.of("c")), OutgoingLiteral.of("a"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(false,
                registry,
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(processor1, processor2, processor3));
        assertThatThrownBy(wiring::resolve).isInstanceOf(CycleException.class);
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

}
