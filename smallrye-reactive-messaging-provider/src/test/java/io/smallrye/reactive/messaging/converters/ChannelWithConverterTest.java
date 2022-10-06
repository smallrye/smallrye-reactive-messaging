package io.smallrye.reactive.messaging.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ChannelWithConverterTest extends WeldTestBaseWithoutTails {

    @Test
    public void testConversionWhenReceivingMessage() {
        addBeanClass(Source.class, ChannelOfMessageOfPerson.class, StringToPersonConverter.class);
        initialize();
        ChannelOfMessageOfPerson sink = get(ChannelOfMessageOfPerson.class);
        assertThat(sink.list()).hasSize(5);
    }

    @Test
    public void testConversionWithSubscriberBuilder() {
        addBeanClass(Source.class, ChannelOfPerson.class, StringToPersonConverter.class);
        initialize();
        ChannelOfPerson sink = get(ChannelOfPerson.class);
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
    public static class ChannelOfMessageOfPerson {
        List<Person> list = new ArrayList<>();

        @Inject
        @Channel("in")
        Multi<Message<Person>> sinkOfMessageOfPerson;

        @PostConstruct
        public void sink() {
            sinkOfMessageOfPerson.onItem().transformToUniAndConcatenate(m -> {
                assertThat(m.getMetadata(Meta.class)).isNotEmpty();
                list.add(m.getPayload());
                return Uni.createFrom().completionStage(m.ack());
            }).toUni().subscribeAsCompletionStage();
        }

        public List<Person> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class ChannelOfPerson {
        List<Person> list = new ArrayList<>();

        @Inject
        @Channel("in")
        Multi<Person> sinkOfPerson;

        @PostConstruct
        public void sink() {
            sinkOfPerson.subscribe().with(p -> list.add(p));
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
