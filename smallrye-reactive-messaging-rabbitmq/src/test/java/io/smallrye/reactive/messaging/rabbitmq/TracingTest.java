package io.smallrye.reactive.messaging.rabbitmq;

import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_DESTINATION_KIND;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_PROTOCOL;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_PROTOCOL_VERSION;
import static io.opentelemetry.semconv.trace.attributes.SemanticAttributes.MESSAGING_SYSTEM;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;

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
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySource;

public class TracingTest extends WeldTestBase {
    private SdkTracerProvider tracerProvider;
    private InMemorySpanExporter spanExporter;

    @BeforeEach
    public void openTelemetry() {
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

    @Test
    void incoming() {
        IncomingTracing tracing = runApplication(commonConfig()
                .with("mp.messaging.incoming.from-rabbitmq.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.from-rabbitmq.queue.name", queue)
                .with("mp.messaging.incoming.from-rabbitmq.exchange.name", exchange)
                .with("mp.messaging.incoming.from-rabbitmq.exchange.routing-keys", routingKeys)
                .with("mp.messaging.incoming.from-rabbitmq.tracing.enabled", true),
                IncomingTracing.class);

        AtomicInteger counter = new AtomicInteger(1);
        usage.produce(exchange, queue, routingKeys, 5, counter::getAndIncrement,
                new AMQP.BasicProperties().builder().expiration("10000").contentType("text/plain").build());
        await().atMost(5, SECONDS).until(() -> tracing.getResults().size() == 5);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(5, spans.size());
            assertEquals(5, spans.stream().map(SpanData::getTraceId).collect(toSet()).size());

            SpanData consumer = spans.get(0);
            assertEquals(SpanKind.CONSUMER, consumer.getKind());
            assertEquals("rabbitmq", consumer.getAttributes().get(MESSAGING_SYSTEM));
            assertNull(consumer.getAttributes().get(MESSAGING_PROTOCOL));
            assertNull(consumer.getAttributes().get(MESSAGING_PROTOCOL_VERSION));
            assertEquals("queue", consumer.getAttributes().get(MESSAGING_DESTINATION_KIND));
            assertEquals(queue, consumer.getAttributes().get(MESSAGING_DESTINATION_NAME));
            assertEquals(queue + " receive", consumer.getName());
        });
    }

    @Test
    void incomingClientPropagate() {
        IncomingTracing tracing = runApplication(commonConfig()
                .with("mp.messaging.incoming.from-rabbitmq.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.from-rabbitmq.queue.name", queue)
                .with("mp.messaging.incoming.from-rabbitmq.exchange.name", exchange)
                .with("mp.messaging.incoming.from-rabbitmq.exchange.routing-keys", routingKeys)
                .with("mp.messaging.incoming.from-rabbitmq.tracing.enabled", true),
                IncomingTracing.class);

        // A Client Span and Propagate the OTel Context
        Map<String, Object> headers = new HashMap<>();
        try (Scope ignored = Context.current().makeCurrent()) {
            Tracer tracer = GlobalOpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging.rabbitmq");
            Span span = tracer.spanBuilder("client").setSpanKind(SpanKind.CLIENT).startSpan();
            Context current = Context.current().with(span);
            GlobalOpenTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(current, headers, Map::put);
            span.end();
        }

        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().expiration("10000").contentType("text/plain")
                .headers(headers).build();

        AtomicInteger counter = new AtomicInteger(1);
        usage.produce(exchange, queue, routingKeys, 5, counter::getAndIncrement, properties);
        await().atMost(5, SECONDS).until(() -> tracing.getResults().size() == 5);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(6, spans.size());
            assertEquals(1, spans.stream().map(SpanData::getTraceId).collect(toSet()).size());
        });
    }

    @Test
    void incomingOutgoing() {
        addBeans(InMemoryConnector.class);

        IncomingOutgoingTracing tracing = runApplication(commonConfig()
                .with("mp.messaging.incoming.generator.connector", InMemoryConnector.CONNECTOR)
                .with("mp.messaging.outgoing.to-rabbitmq.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.to-rabbitmq.queue.name", queue)
                .with("mp.messaging.outgoing.to-rabbitmq.exchange.name", exchange)
                .with("mp.messaging.outgoing.to-rabbitmq.exchange.routing-keys", routingKeys)
                .with("mp.messaging.outgoing.to-rabbitmq.tracing.enabled", true)
                .with("mp.messaging.incoming.from-rabbitmq.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.from-rabbitmq.queue.name", queue)
                .with("mp.messaging.incoming.from-rabbitmq.exchange.name", exchange)
                .with("mp.messaging.incoming.from-rabbitmq.exchange.routing-keys", routingKeys)
                .with("mp.messaging.incoming.from-rabbitmq.tracing.enabled", true),
                IncomingOutgoingTracing.class);

        InMemoryConnector inMemoryConnector = CDI.current()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        InMemorySource<Object> input = inMemoryConnector.source("generator");
        for (int i = 1; i <= 5; i++) {
            input.send(i);
        }
        await().atMost(5, SECONDS).until(() -> tracing.getResults().size() == 5);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(10, spans.size());

            List<SpanData> parentSpans = spans.stream()
                    .filter(spanData -> spanData.getParentSpanId().equals(SpanId.getInvalid())).collect(toList());
            assertEquals(5, parentSpans.size());

            for (SpanData parentSpan : parentSpans) {
                assertEquals(1,
                        spans.stream().filter(spanData -> spanData.getParentSpanId().equals(parentSpan.getSpanId())).count());
            }
        });
    }

    @Test
    void incomingOutgoingSink() {
        IncomingOutgoingSinkTracing tracing = runApplication(commonConfig()
                .with("mp.messaging.incoming.from-rabbitmq.connector", RabbitMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.from-rabbitmq.queue.name", queue)
                .with("mp.messaging.incoming.from-rabbitmq.exchange.name", exchange)
                .with("mp.messaging.incoming.from-rabbitmq.exchange.routing-keys", routingKeys)
                .with("mp.messaging.incoming.from-rabbitmq.tracing.enabled", true),
                IncomingOutgoingSinkTracing.class);

        AtomicInteger counter = new AtomicInteger(1);
        usage.produce(exchange, queue, routingKeys, 5, counter::getAndIncrement,
                new AMQP.BasicProperties().builder().expiration("10000").contentType("text/plain").build());
        await().atMost(5, SECONDS).until(() -> tracing.getResults().size() == 5);

        CompletableResultCode completableResultCode = tracerProvider.forceFlush();
        completableResultCode.whenComplete(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertEquals(5, spans.size());
            assertEquals(5, spans.stream().map(SpanData::getTraceId).collect(toSet()).size());
        });
    }

    @ApplicationScoped
    static class IncomingTracing {
        private final List<String> results = new ArrayList<>();

        @Incoming("from-rabbitmq")
        public void process(String input) {
            results.add(input);
        }

        public List<String> getResults() {
            return results;
        }
    }

    @ApplicationScoped
    static class IncomingOutgoingTracing {
        private final List<String> results = new ArrayList<>();

        @Incoming("generator")
        @Outgoing("to-rabbitmq")
        public Integer process(Integer input) {
            return input;
        }

        @Incoming("from-rabbitmq")
        public void results(String input) {
            results.add(input);
        }

        public List<String> getResults() {
            return results;
        }
    }

    @ApplicationScoped
    static class IncomingOutgoingSinkTracing {
        private final List<String> results = new ArrayList<>();

        @Incoming("from-rabbitmq")
        @Outgoing("sink")
        public String incoming(String input) {
            return input;
        }

        // TODO - Should we generate spans between the internal sink?
        @Incoming("sink")
        public void sink(String input) {
            results.add(input);
        }

        public List<String> getResults() {
            return results;
        }
    }
}
