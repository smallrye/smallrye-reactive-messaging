package io.smallrye.reactive.messaging.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ProcessorWithConverterTest extends WeldTestBaseWithoutTails {

    @Test
    public void testConversionWhenReceivingPayload() {
        addBeanClass(Source.class, Sink.class, StringToPersonConverter.class, PayloadProcessor.class);
        initialize();
        Sink sink = get(Sink.class);
        assertThat(sink.list()).hasSize(5);
    }

    @Test
    public void testMissingConverter() {
        addBeanClass(Source.class, Sink.class, PayloadProcessor.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        assertThat(sink.list()).hasSize(0);
        assertThat(source.nacks()).isEqualTo(5);
        assertThat(source.acks()).isEqualTo(0);
    }

    @Test
    public void testConversionWhenReceivingMessage() {
        addBeanClass(Source.class, Sink.class, StringToPersonConverter.class, MessageProcessor.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        assertThat(sink.list()).hasSize(5);
        assertThat(source.acks()).isEqualTo(5);
        assertThat(source.nacks()).isEqualTo(0);
    }

    @Test
    public void testConversionWithProcessorBuilder() {
        addBeanClass(Source.class, Sink.class, StringToPersonConverter.class, MessageProcessorBuilder.class);
        initialize();
        Sink sink = get(Sink.class);
        assertThat(sink.list()).hasSize(5);
    }

    @Test
    public void testConversionWithPayloadStream() {
        addBeanClass(Source.class, Sink.class, StringToPersonConverter.class, StreamPayloadTransformer.class);
        initialize();
        Sink sink = get(Sink.class);
        assertThat(sink.list()).hasSize(5);
    }

    @Test
    public void testConversionWithMessageStream() {
        addBeanClass(Source.class, Sink.class, StringToPersonConverter.class, StreamMessageTransformer.class);
        initialize();
        Sink sink = get(Sink.class);
        assertThat(sink.list()).hasSize(5);
    }

    @ApplicationScoped
    static class StringToPersonConverter implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return target == Person.class && in.getPayload().getClass() == String.class;
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            return in.withPayload(new Person((String) in.getPayload())).addMetadata(new Meta());
        }
    }

    @ApplicationScoped
    public static class Source {

        private final AtomicInteger acks = new AtomicInteger();
        private final AtomicInteger nacks = new AtomicInteger();

        @Outgoing("in")
        public Multi<Message<String>> source() {
            return Multi.createFrom().items("Luke", "Leia", "Neo", "Morpheus", "Trinity")
                    .map(s -> Message.of(s, () -> {
                        acks.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    }, t -> {
                        nacks.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    }));
        }

        public int acks() {
            return acks.get();
        }

        public int nacks() {
            return nacks.get();
        }
    }

    @ApplicationScoped
    public static class Sink {
        List<Person> list = new ArrayList<>();

        @Incoming("out")
        public void sink(Person p) {
            list.add(p);
        }

        public List<Person> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class PayloadProcessor {

        @Incoming("in")
        @Outgoing("out")
        public Person process(Person p) {
            return new Person(p.name.toUpperCase());
        }

    }

    @ApplicationScoped
    public static class MessageProcessor {

        @Incoming("in")
        @Outgoing("out")
        public Message<Person> process(Message<Person> p) {
            assertThat(p.getMetadata(Meta.class)).isNotEmpty();
            return p.withPayload(new Person(p.getPayload().name.toUpperCase()));
        }

    }

    @ApplicationScoped
    public static class MessageProcessorBuilder {

        @Incoming("in")
        @Outgoing("out")
        public ProcessorBuilder<Message<Person>, Message<Person>> process() {
            return ReactiveStreams.<Message<Person>> builder()
                    .peek(p -> assertThat(p.getMetadata(Meta.class)).isNotEmpty())
                    .map(p -> p.withPayload(new Person(p.getPayload().name.toUpperCase())));
        }

    }

    @ApplicationScoped
    public static class StreamPayloadTransformer {
        @Incoming("in")
        @Outgoing("out")
        public Multi<Person> process(Multi<Person> multi) {
            return multi
                    .map(p -> new Person(p.name.toUpperCase()));
        }
    }

    @ApplicationScoped
    public static class StreamMessageTransformer {
        @Incoming("in")
        @Outgoing("out")
        public Multi<Message<Person>> process(Multi<Message<Person>> multi) {
            return multi
                    .invoke(p -> assertThat(p.getMetadata(Meta.class)).isNotEmpty())
                    .map(p -> p.withPayload(new Person(p.getPayload().name.toUpperCase())));
        }
    }

    public static class Person {
        public final String name;

        Person(String name) {
            this.name = name;
        }
    }

    public static class Meta {

    }
}
