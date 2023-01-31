package io.smallrye.reactive.messaging.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class SubscriberWithConverterTest extends WeldTestBaseWithoutTails {

    @Test
    public void testConversionWhenReceivingPayload() {
        addBeanClass(Source.class, SinkOfPerson.class, StringToPersonConverter.class);
        initialize();
        SinkOfPerson sink = get(SinkOfPerson.class);
        assertThat(sink.list()).hasSize(5);
    }

    @Test
    public void testConversionWhenReceivingMessage() {
        addBeanClass(Source.class, SinkOfMessageOfPerson.class, StringToPersonConverter.class);
        initialize();
        SinkOfMessageOfPerson sink = get(SinkOfMessageOfPerson.class);
        assertThat(sink.list()).hasSize(5);
    }

    @Test
    public void testConversionWithSubscriberBuilder() {
        addBeanClass(Source.class, SubscriberOfPerson.class, StringToPersonConverter.class);
        initialize();
        SubscriberOfPerson sink = get(SubscriberOfPerson.class);
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
        @Outgoing("in")
        public Multi<String> source() {
            return Multi.createFrom().items("Luke", "Leia", "Neo", "Morpheus", "Trinity");
        }
    }

    @ApplicationScoped
    public static class SinkOfPerson {
        List<Person> list = new ArrayList<>();

        @Incoming("in")
        public void sink(Person p) {
            list.add(p);
        }

        public List<Person> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class SinkOfMessageOfPerson {
        List<Person> list = new ArrayList<>();

        @Incoming("in")
        public CompletionStage<Void> sink(Message<Person> m) {
            assertThat(m.getMetadata(Meta.class)).isNotEmpty();
            list.add(m.getPayload());
            return m.ack();
        }

        public List<Person> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class SubscriberOfPerson {
        List<Person> list = new ArrayList<>();

        @Incoming("in")
        public SubscriberBuilder<Person, Void> sink() {
            return ReactiveStreams.<Person> builder()
                    .forEach(p -> list.add(p));
        }

        public List<Person> list() {
            return list;
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
