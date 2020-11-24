package io.smallrye.reactive.messaging.wiring;

import static io.smallrye.reactive.messaging.extension.MediatorManager.STRICT_MODE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;

import javax.enterprise.inject.spi.Bean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.DefaultMediatorConfiguration;
import io.smallrye.reactive.messaging.extension.ChannelConfiguration;
import io.smallrye.reactive.messaging.extension.EmitterConfiguration;

@SuppressWarnings("rawtypes")
class WiringTest {

    @AfterEach
    public void cleanup() {
        System.setProperty(STRICT_MODE_PROPERTY, "false");
    }

    @Test
    public void testEmptyGraph() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Wiring wiring = new Wiring();
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
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
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(),
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
        System.setProperty(STRICT_MODE_PROPERTY, "true");

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
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(),
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
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(),
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
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(),
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
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(),
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
        System.setProperty(STRICT_MODE_PROPERTY, "true");
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(2);
        assertThat(graph.getUnresolvedComponents()).hasSize(0);
        assertThat(graph.isClosed()).isFalse();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1).allSatisfy(e -> {
            assertThat(e).isInstanceOf(NotClosedGraph.class);
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
        when(registry.getOutgoingNames()).thenReturn(Collections.singleton("b"));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        Wiring wiring = new Wiring();
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(),
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
        when(registry.getOutgoingNames()).thenReturn(Collections.singleton("b"));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        Wiring wiring = new Wiring();
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
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
        when(registry.getOutgoingNames()).thenReturn(Collections.singleton("a"));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        Wiring wiring = new Wiring();
        wiring.prepare(registry, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
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
        wiring.prepare(registry, Collections.singletonList(new EmitterConfiguration("a", false, null, null)),
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
        wiring.prepare(registry,
                Collections.singletonList(new EmitterConfiguration("a", false, null, null)),
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
        wiring.prepare(registry,
                Collections.singletonList(new EmitterConfiguration("a", false, null, null)),
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
        wiring.prepare(registry,
                Collections.singletonList(new EmitterConfiguration("a", false, null, null)),
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
        wiring.prepare(registry,
                Collections.singletonList(new EmitterConfiguration("a", false, null, null)),
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
        wiring.prepare(registry,
                Collections.singletonList(new EmitterConfiguration("a", false, null, null)),
                Collections.singletonList(new ChannelConfiguration("a")),
                Collections.emptyList());
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(2);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.incomings()).containsExactly("a"));
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
