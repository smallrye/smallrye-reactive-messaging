package io.smallrye.reactive.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.Merge;

public class InheritanceTest extends WeldTestBaseWithoutTails {

    @Test
    public void test() {
        addBeanClass(Foo.class);
        initialize();

        Foo foo = get(Foo.class);
        foo.emit();

        assertThat(foo.list())
                .hasSize(1)
                .allSatisfy(m -> assertThat(m.content).isEqualTo("hello"));
    }

    interface MyMessageTypeListener extends SimpleListener<MyMessageType> {

    }

    @FunctionalInterface
    interface SimpleListener<T> {
        void onMessage(T message);
    }

    @ApplicationScoped
    static class Foo implements MyMessageTypeListener {
        private final List<MyMessageType> list = new ArrayList<>();
        @Inject
        @Channel("in")
        Emitter<MyMessageType> emitter;

        @Override
        @Incoming("in")
        @Merge
        public void onMessage(MyMessageType message) {
            list.add(message);
        }

        public void emit() {
            emitter.send(new MyMessageType("hello"));
        }

        public List<MyMessageType> list() {
            return list;
        }
    }

    private static class MyMessageType {

        String content;

        public MyMessageType(String content) {
            this.content = content;
        }
    }
}
