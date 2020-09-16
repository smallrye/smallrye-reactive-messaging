package io.smallrye.reactive.messaging.converters;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import javax.enterprise.context.ApplicationScoped;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class MultipleConverterTest extends WeldTestBaseWithoutTails {

    @Test
    public void testWithMultipleCompetingConverters() {
        addBeanClass(Source.class, Sink.class, StringToPersonConverterWithDefaultPriority.class, StringToPersonConverterWithHighPriority.class, PayloadProcessor.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        assertThat(sink.list()).hasSize(5);
        assertThat(source.acks()).isEqualTo(5);
        assertThat(source.nacks()).isEqualTo(0);
        assertThat(sink.list().stream().map(p -> p.name).collect(Collectors.toList()))
            .containsExactly("LUKE", "LEIA", "NEO", "MORPHEUS", "TRINITY");
    }

    @Test
    public void testWithMultipleCompetingConvertersReversed() {
        addBeanClass(Source.class, Sink.class, StringToPersonConverterWithHighPriority.class, StringToPersonConverterWithDefaultPriority.class, PayloadProcessor.class);
        initialize();
        Sink sink = get(Sink.class);
        Source source = get(Source.class);
        assertThat(sink.list()).hasSize(5);
        assertThat(source.acks()).isEqualTo(5);
        assertThat(source.nacks()).isEqualTo(0);
        assertThat(sink.list().stream().map(p -> p.name).collect(Collectors.toList()))
            .containsExactly("LUKE", "LEIA", "NEO", "MORPHEUS", "TRINITY");
    }

    @ApplicationScoped
    static class StringToPersonConverterWithDefaultPriority implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return target == Person.class && in.getPayload().getClass() == String.class;
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            return in.withPayload(new Person((String) in.getPayload()));
        }
    }

    @ApplicationScoped
    static class StringToPersonConverterWithHighPriority implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return target == Person.class && in.getPayload().getClass() == String.class;
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            String payload = (String) in.getPayload();
            return in.withPayload(new Person(payload.toUpperCase()));
        }

        @Override
        public int getPriority() {
            return 200;
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

    public static class Person {
        public final String name;

        Person(String name) {
            this.name = name;
        }
    }

    public static class Meta {

    }
}
