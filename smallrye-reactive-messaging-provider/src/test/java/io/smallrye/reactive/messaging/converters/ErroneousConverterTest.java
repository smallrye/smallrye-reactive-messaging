package io.smallrye.reactive.messaging.converters;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ErroneousConverterTest extends WeldTestBaseWithoutTails {

    @Test
    public void testConverterThrowingExceptionOnAccept() {
        addBeanClass(Source.class, Sink.class, PayloadProcessor.class, BadConverterThrowingExceptionOnAccept.class);
        assertThatThrownBy(this::initialize)
                .isInstanceOf(DeploymentException.class)
                .hasStackTraceContaining("boom");
    }

    @Test
    public void testConverterThrowingExceptionOnConvert() {
        addBeanClass(Source.class, Sink.class, PayloadProcessor.class, BadConverterThrowingExceptionOnConvert.class);
        assertThatThrownBy(this::initialize)
                .isInstanceOf(DeploymentException.class)
                .hasStackTraceContaining("boom");
    }

    @ApplicationScoped
    static class BadConverterThrowingExceptionOnAccept implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            throw new IllegalArgumentException("boom");
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            return in.withPayload(new Person((String) in.getPayload())).addMetadata(new Meta());
        }
    }

    @ApplicationScoped
    static class BadConverterThrowingExceptionOnConvert implements MessageConverter {

        @Override
        public boolean canConvert(Message<?> in, Type target) {
            return target == Person.class && in.getPayload().getClass() == String.class;
        }

        @Override
        public Message<?> convert(Message<?> in, Type target) {
            if (in.getPayload().toString().startsWith("N") || in.getPayload().toString().startsWith("M")) {
                throw new IllegalArgumentException("boom");
            }
            return in.withPayload(new Person((String) in.getPayload())).addMetadata(new Meta());
        }
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

    public static class Person {
        public final String name;

        Person(String name) {
            this.name = name;
        }
    }

    public static class Meta {

    }
}
