package io.smallrye.reactive.messaging.jms.tracing;

import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_SYSTEM;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessageMetadata;
import io.smallrye.reactive.messaging.jms.JmsConnector;
import io.smallrye.reactive.messaging.jms.PayloadConsumerBean;
import io.smallrye.reactive.messaging.jms.ProducerBean;
import io.smallrye.reactive.messaging.support.JmsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class TracingPropagationTest extends JmsTestBase {
    private SdkTracerProvider tracerProvider;
    private InMemorySpanExporter spanExporter;

    private JMSContext jms;
    private ActiveMQJMSConnectionFactory factory;

    @BeforeEach
    public void setup() {
        factory = new ActiveMQJMSConnectionFactory(
                "tcp://localhost:61616",
                null, null);
        jms = factory.createContext();

        GlobalOpenTelemetry.resetForTest();

        spanExporter = InMemorySpanExporter.create();
        SpanProcessor spanProcessor = SimpleSpanProcessor.create(spanExporter);

        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .setSampler(Sampler.alwaysOn())
                .build();

        OpenTelemetrySdk.builder()
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();
    }

    @AfterEach
    public void close() {
        jms.close();
        factory.close();
    }

    @AfterAll
    static void shutdown() {
        GlobalOpenTelemetry.resetForTest();
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFromAppToJms() {
        String queue = "queue-one";
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.outgoing.queue-one.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.stuff.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.stuff.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(MyAppReceivingData.class, ProducerBean.class);

        MyAppReceivingData bean = container.select(MyAppReceivingData.class).get();
        await().until(() -> bean.results().size() >= 10);
        assertThat(bean.results())
                .extracting(m -> m.getMetadata(IncomingJmsMessageMetadata.class).get())
                .allSatisfy(m -> assertThat(m.getStringProperty("traceparent")).isNotNull())
                .extracting(m -> Integer.parseInt(m.getBody(String.class)))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(20, spans.size());

        assertEquals(10, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

        SpanData span = spans.get(0);
        assertEquals(SpanKind.PRODUCER, span.getKind());
        assertEquals(3, span.getAttributes().size());
        assertEquals("jms", span.getAttributes().get(MESSAGING_SYSTEM));
        assertEquals("publish", span.getAttributes().get(MESSAGING_OPERATION));
        assertEquals("ActiveMQQueue[" + queue + "]", span.getAttributes().get(MESSAGING_DESTINATION_NAME));
        assertEquals("ActiveMQQueue[" + queue + "] publish", span.getName());
    }

    @Test
    public void testFromJmsToAppToJms() {
        String queue = "queue-one";
        String parentQueue = queue + "-parent";
        String resultQueue = queue + "-result";
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.incoming.source.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.source.destination", parentQueue);
        map.put("mp.messaging.outgoing.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.outgoing.jms.destination", resultQueue);
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(MyAppProcessingData.class);

        List<jakarta.jms.Message> results = new CopyOnWriteArrayList<>();
        Queue result = jms.createQueue(resultQueue);
        JMSConsumer sink = jms.createConsumer(result);
        sink.setMessageListener(results::add);

        MyAppProcessingData bean = container.select(MyAppProcessingData.class).get();

        JMSProducer producer = jms.createProducer();
        Queue parent = jms.createQueue(parentQueue);
        for (int i = 0; i < 10; i++) {
            producer.send(parent, i);
        }

        await().until(() -> results.size() >= 10);
        assertThat(results).extracting(m -> Integer.valueOf(m.getBody(String.class))).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9,
                10);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(20, spans.size());

        List<SpanData> parentSpans = spans.stream()
                .filter(spanData -> spanData.getParentSpanId().equals(SpanId.getInvalid()))
                .collect(toList());
        assertEquals(10, parentSpans.size());

        //        for (SpanData parentSpan : parentSpans) {
        //            assertEquals(1,
        //                    spans.stream().filter(spanData -> spanData.getParentSpanId().equals(parentSpan.getSpanId())).count());
        //        }

        SpanData consumer = parentSpans.get(0);
        assertEquals(SpanKind.CONSUMER, consumer.getKind());
        assertEquals(3, consumer.getAttributes().size());
        assertEquals("jms", consumer.getAttributes().get(MESSAGING_SYSTEM));
        assertEquals("receive", consumer.getAttributes().get(MESSAGING_OPERATION));
        assertEquals(parentQueue, consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
        assertEquals(parentQueue + " receive", consumer.getName());

        for (SpanData span : spans) {
            System.out.println(span.getKind() + " " + span.getSpanId() + " -> " + span.getParentSpanId());
        }
        SpanData producerSpan = spans.stream().filter(spanData -> spanData.getParentSpanId().equals(consumer.getSpanId()))
                .findFirst().get();
        assertEquals(SpanKind.PRODUCER, producerSpan.getKind());
        assertEquals(3, producerSpan.getAttributes().size());
        assertEquals("jms", producerSpan.getAttributes().get(MESSAGING_SYSTEM));
        assertEquals("publish", producerSpan.getAttributes().get(MESSAGING_OPERATION));
        assertEquals("ActiveMQQueue[" + resultQueue + "]", producerSpan.getAttributes().get(MESSAGING_DESTINATION_NAME));
        assertEquals("publish", producerSpan.getAttributes().get(MESSAGING_OPERATION));
        assertEquals("ActiveMQQueue[" + resultQueue + "] publish", producerSpan.getName());
    }

    @Test
    public void testFromJmsToAppWithParentSpan() {
        String parentTopic = "queue-one-parent";
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", parentTopic);
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();

        Map<String, String> properties = new HashMap<>();
        try (Scope ignored = Context.current().makeCurrent()) {
            Tracer tracer = GlobalOpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging");
            Span span = tracer.spanBuilder("producer").setSpanKind(SpanKind.PRODUCER).startSpan();
            Context current = Context.current().with(span);
            GlobalOpenTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(current, properties, (carrier, key, value) -> carrier.put(key, value));
            span.end();
        }

        JMSProducer producer = jms.createProducer();
        Queue queue = jms.createQueue(parentTopic);
        for (int i = 0; i < 10; i++) {
            ObjectMessage msg = jms.createObjectMessage(i);
            properties.forEach((k, v) -> {
                try {
                    msg.setObjectProperty(k, v);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            producer.send(queue, msg);
        }

        await().until(() -> bean.list().size() >= 10);

        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);
        // 1 Parent, 10 Children
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(11, spans.size());

        // All should use the same Trace
        assertEquals(1, spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet()).size());

        List<SpanData> parentSpans = spans.stream()
                .filter(spanData -> spanData.getParentSpanId().equals(SpanId.getInvalid())).collect(toList());
        assertEquals(1, parentSpans.size());

        for (SpanData parentSpan : parentSpans) {
            assertEquals(10,
                    spans.stream().filter(spanData -> spanData.getParentSpanId().equals(parentSpan.getSpanId())).count());
        }

        SpanData producerSpan = parentSpans.get(0);
        assertEquals(SpanKind.PRODUCER, producerSpan.getKind());

        SpanData consumer = spans.stream().filter(spanData -> spanData.getParentSpanId().equals(producerSpan.getSpanId()))
                .findFirst().get();
        assertEquals(3, consumer.getAttributes().size());
        assertEquals("jms", consumer.getAttributes().get(MESSAGING_SYSTEM));
        assertEquals("receive", consumer.getAttributes().get(MESSAGING_OPERATION));
        assertEquals(parentTopic, consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
        assertEquals(parentTopic + " receive", consumer.getName());

    }

    @Test
    public void testFromJmsToAppWithNoParent() {
        Map<String, Object> map = new HashMap<>();
        map.put("mp.messaging.incoming.jms.connector", JmsConnector.CONNECTOR_NAME);
        map.put("mp.messaging.incoming.jms.destination", "queue-one");
        MapBasedConfig config = new MapBasedConfig(map);
        addConfig(config);
        WeldContainer container = deploy(PayloadConsumerBean.class);

        PayloadConsumerBean bean = container.select(PayloadConsumerBean.class).get();

        JMSProducer producer = jms.createProducer();
        for (int i = 0; i < 10; i++) {
            producer.send(jms.createQueue("queue-one"), i);
        }

        await().until(() -> bean.list().size() >= 10);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.join(10, TimeUnit.SECONDS);
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(10).allSatisfy(span -> {
            assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(span.getSpanId()).isNotEqualTo(SpanId.getInvalid());
            assertThat(span.getParentSpanId()).isEqualTo(SpanId.getInvalid());
        });
    }

    @ApplicationScoped
    public static class MyAppProcessingData {

        private final List<Integer> list = new CopyOnWriteArrayList<>();

        @Incoming("source")
        @Outgoing("jms")
        public Message<Integer> processMessage(Message<Integer> input) {
            list.add(input.getPayload());
            return input.withPayload(input.getPayload() + 1);
        }

        public List<Integer> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MyAppReceivingData {
        private final List<Message<Integer>> results = new CopyOnWriteArrayList<>();

        @Incoming("stuff")
        public CompletionStage<Void> consume(Message<Integer> input) {
            results.add(input);
            return input.ack();
        }

        public List<Message<Integer>> results() {
            return results;
        }
    }
}
