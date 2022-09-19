package io.smallrye.reactive.messaging.providers.wiring;

import static io.smallrye.reactive.messaging.annotations.EmitterFactoryFor.Literal.EMITTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;

import jakarta.enterprise.inject.spi.Bean;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.providers.DefaultEmitterConfiguration;
import io.smallrye.reactive.messaging.providers.DefaultMediatorConfiguration;
import io.smallrye.reactive.messaging.providers.extension.ChannelConfiguration;

@SuppressWarnings("rawtypes")
public class WiringBroadcastTest {

    /**
     * Emitter emitting on the channel "a" with broadcast and two downstream subscribers (1 channel and one processor)
     * The @Broadcast does not specify the number of subscribers.
     * <p>
     * emitter - {a} -> processor - {b} -> channel
     * emitter - {a} -> channel
     */
    @Test
    public void testEmitterWithImmediateBroadcast() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        EmitterConfiguration ec = new DefaultEmitterConfiguration("a", EMITTER, null, BroadcastLiteral.of(0));
        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.singletonList(ec), Arrays.asList(cc1, cc2),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();

        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(2);
    }

    /**
     * Emitter emitting on the channel "a" with broadcast and two downstream subscribers (1 channel and one processor)
     * The @Broadcast sets the number of subscribers to 2.
     * <p>
     * emitter - {a} -> processor - {b} -> channel
     * -> channel
     */
    @Test
    public void testEmitterWithBroadcastAndExactNumberOfSubscribers() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        EmitterConfiguration ec = new DefaultEmitterConfiguration("a", EMITTER, null, BroadcastLiteral.of(2));
        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.singletonList(ec), Arrays.asList(cc1, cc2),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(2);
    }

    /**
     * Emitter emitting on the channel "a" with broadcast and two downstream subscribers (1 channel and one processor)
     * The @Broadcast sets the number of subscribers to 1.
     * <p>
     * emitter - {a} -> processor - {b} -> channel
     * -> channel
     */
    @Test
    public void testEmitterWithBroadcastAndMoreSubscribersThanConfigured() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        EmitterConfiguration ec = new DefaultEmitterConfiguration("a", EMITTER, null, BroadcastLiteral.of(1));
        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.singletonList(ec), Arrays.asList(cc1, cc2),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1).allSatisfy(e -> assertThat(e)
                .isInstanceOf(UnsatisfiedBroadcastException.class)
                .hasMessageContaining("1", "2"));
    }

    /**
     * Emitter emitting on the channel "a" with broadcast and two downstream subscribers (1 channel and one processor)
     * The @Broadcast sets the number of subscribers to 3.
     * <p>
     * emitter - {a} -> processor - {b} -> channel
     * -> channel
     */
    @Test
    public void testEmitterWithUnsatisfiedBroadcast() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        EmitterConfiguration ec = new DefaultEmitterConfiguration("a", EMITTER, null, BroadcastLiteral.of(3));
        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.singletonList(ec), Arrays.asList(cc1, cc2),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1)
                .allSatisfy(e -> assertThat(e).isInstanceOf(UnsatisfiedBroadcastException.class));
    }

    @Test
    public void testWithInvalidEmitterBroadcastAndHint() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        EmitterConfiguration ec = new DefaultEmitterConfiguration("a", EMITTER, null, null);
        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.singletonList(ec), Arrays.asList(cc1, cc2),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();

        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1)
                .allSatisfy(t -> assertThat(t).isInstanceOf(TooManyDownstreamCandidatesException.class)
                        .hasMessageContaining("channel:'a'")
                        .hasMessageContaining("@Broadcast"));

    }

    /**
     * Producer method emitting on the channel "a" with broadcast and two downstream subscribers (1 channel and one processor)
     * The @Broadcast does not set the number of subscriber
     * <p>
     * produce - {a} -> processor - {b} -> channel
     * -> channel
     */
    @Test
    public void testWithPublisherMethodWithImmediateBroadcast() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producerWithBroadcast"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(2);
    }

    /**
     * Producer method emitting on the channel "a" with broadcast and two downstream subscribers (1 channel and one processor)
     * The @Broadcast sets the number of subscribers to 2.
     * <p>
     * produce - {a} -> processor - {b} -> channel
     * -> channel
     */
    @Test
    public void testWithPublisherMethodWithBroadcastAndExactNumberOfSubscribers() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producerWithBroadcast2"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(2);
    }

    /**
     * Producer method emitting on the channel "a" with broadcast and three downstream subscribers (2 channels and one
     * processor)
     * The @Broadcast sets the number of subscribers to 2.
     * <p>
     * produce - {a} -> processor - {b} -> channel
     * -> channel
     * -> channel
     */
    @Test
    public void testWithPublisherMethodWithBroadcastAnMoreSubscribersThanConfigured() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producerWithBroadcast2"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");
        ChannelConfiguration cc3 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2, cc3),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(5);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1)
                .allSatisfy(e -> assertThat(e).isInstanceOf(UnsatisfiedBroadcastException.class));
    }

    /**
     * Producer method emitting on the channel "a" with broadcast and two downstream subscribers (1 channel and one processor)
     * The @Broadcast sets the number of subscribers to 3.
     * <p>
     * produce - {a} -> processor - {b} -> channel
     * -> channel
     */
    @Test
    public void testWithPublisherMethodWithUnsatisfiedBroadcast() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producerWithBroadcast3"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1)
                .allSatisfy(e -> assertThat(e).isInstanceOf(UnsatisfiedBroadcastException.class));
    }

    /**
     * Test missing broadcast on publisher method
     */
    @Test
    public void testWithPublishingMethodAndMissingBroadcastAndHint() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();

        assertThat(graph.getWiringErrors()).hasSize(1)
                .allSatisfy(e -> assertThat(e).isInstanceOf(TooManyDownstreamCandidatesException.class)
                        .hasMessageContaining("@Broadcast"));

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(2);
    }

    /**
     * Producer method emitting on the channel "a" and a process method with broadcast and two downstream subscribers (2
     * channels)
     * The @Broadcast does not set the number of subscriber
     * <p>
     * produce - {a} -> processor - {b} -> channel
     * -> channel
     */
    @Test
    public void testWithProcessorMethodWithImmediateBroadcast() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("processWithBroadcast"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("b");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(2);
    }

    /**
     * Producer method emitting on the channel "a" and a process method with broadcast and two downstream subscribers (2
     * channels)
     * The @Broadcast sets the number of subscribers to 2.
     * <p>
     * produce - {a} -> processor - {b} -> channel
     * -> channel
     */
    @Test
    public void testWithProcessorMethodWithBroadcastAndExactNumberOfSubscribers() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("processWithBroadcast2"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("b");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isFalse();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(2);
    }

    /**
     * Producer method emitting on the channel "a" and a process method with broadcast and three downstream subscribers (3
     * channels)
     * The @Broadcast sets the number of subscribers to 2.
     * <p>
     * produce - {a} -> processor - {b} -> channel
     * -> channel
     * -> channel
     */
    @Test
    public void testWithProcessorMethodWithBroadcastAnMoreSubscribersThanConfigured() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("processWithBroadcast2"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("b");
        ChannelConfiguration cc3 = new ChannelConfiguration("b");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2, cc3),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(5);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1)
                .allSatisfy(e -> assertThat(e).isInstanceOf(UnsatisfiedBroadcastException.class));
    }

    /**
     * Producer method emitting on the channel "a" and a process method with broadcast and two downstream subscribers (2
     * channels)
     * The @Broadcast sets the number of subscribers to 3.
     * <p>
     * produce - {a} -> processor - {b} -> channel
     * -> channel
     */
    @Test
    public void testWithProcessorMethodWithUnsatisfiedBroadcast() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("processWithBroadcast3"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("b");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1)
                .allSatisfy(e -> assertThat(e).isInstanceOf(UnsatisfiedBroadcastException.class));
    }

    /**
     * Test missing broadcast on producer method
     */
    @Test
    public void testWithProducerMethodAndMissingBroadcastAndHint() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration producer = new DefaultMediatorConfiguration(getMethod("producer"), bean);
        producer.compute(Collections.emptyList(), OutgoingLiteral.of("a"), null);
        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("b");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Arrays.asList(processor, producer));
        Graph graph = wiring.resolve();
        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getResolvedComponents()).hasSize(4);
        assertThat(graph.isClosed()).isTrue();

        assertThat(graph.getWiringErrors()).hasSize(1)
                .allSatisfy(e -> assertThat(e).isInstanceOf(TooManyDownstreamCandidatesException.class)
                        .hasMessageContaining("@Broadcast"));

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(2);
    }

    /**
     * connector - {a} -> process -> channel
     * -> channel
     */
    @Test
    public void testWithConnectorWithBroadcast() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", true));

        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();

        assertThat(graph.hasWiringErrors()).isFalse();
        assertThat(graph.getWiringErrors()).hasSize(0);
        assertThat(graph.isClosed()).isTrue();

        assertThat(graph.getInbound()).hasSize(1).allSatisfy(pc -> assertThat(pc.outgoing()).contains("a"));
        assertThat(graph.getOutbound()).hasSize(2);
    }

    /**
     * connector - {a} -> process -> channel
     * -> channel
     * <p>
     * the connector does not have broadcast enabled.
     */
    @Test
    public void testWithMissingConnectorBroadcastAndHint() {
        ChannelRegistry registry = mock(ChannelRegistry.class);
        when(registry.getIncomingChannels()).thenReturn(Collections.singletonMap("a", false));
        Bean bean = mock(Bean.class);
        when(bean.getBeanClass()).thenReturn(WiringTest.class);

        DefaultMediatorConfiguration processor = new DefaultMediatorConfiguration(getMethod("process"), bean);
        processor.compute(Collections.singletonList(IncomingLiteral.of("a")), OutgoingLiteral.of("b"), null);

        ChannelConfiguration cc1 = new ChannelConfiguration("b");
        ChannelConfiguration cc2 = new ChannelConfiguration("a");

        Wiring wiring = new Wiring();
        wiring.prepare(false, registry, Collections.emptyList(), Arrays.asList(cc1, cc2),
                Collections.singletonList(processor));
        Graph graph = wiring.resolve();

        assertThat(graph.hasWiringErrors()).isTrue();
        assertThat(graph.getWiringErrors()).hasSize(1)
                .allSatisfy(t -> assertThat(t).isInstanceOf(TooManyDownstreamCandidatesException.class)
                        .hasMessageContaining("channel:'a'")
                        .hasMessageContaining("mp.messaging.incoming.a.broadcast=true"));

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

    @Broadcast
    public String producerWithBroadcast() {
        // ...
        return "hello";
    }

    @Broadcast(2)
    public String producerWithBroadcast2() {
        // ...
        return "hello";
    }

    @Broadcast(3)
    public String producerWithBroadcast3() {
        // ...
        return "hello";
    }

    @Broadcast
    public String processWithBroadcast(String s) {
        return s;
    }

    @Broadcast(2)
    public String processWithBroadcast2(String s) {
        return s;
    }

    @Broadcast(3)
    public String processWithBroadcast3(String s) {
        return s;
    }

}
