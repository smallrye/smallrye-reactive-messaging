package io.smallrye.reactive.messaging.split;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.split.MultiSplitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class MultiSplitterTest extends WeldTestBaseWithoutTails {

    @Test
    void testSplitOfPayloads() {
        addBeanClass(Sink.class, PayloadSource.class, SplitterMulti.class);

        // Failures are captured during the initialization because these tests are not async (the sources are immediate)

        try {
            initialize();
        } catch (Exception e) {
            Assertions.fail("Initialization not expected to fail", e);
            return;
        }

        Sink sink = get(Sink.class);
        await().until(() -> sink.listSink1().size() == 2
                && sink.listSink2().size() == 2
                && sink.listSink3().size() == 2);
        assertThat(sink.listSink1()).contains("ABC", "DEF");
        assertThat(sink.listSink2()).contains("abc", "def");
        assertThat(sink.listSink3()).contains("AbC", "DeF");
    }

    @Test
    void testSplitOfPayloadsWithMatchingChannelNames() {
        addBeanClass(Sink.class, PayloadSource.class, SplitterMultiWithChannelNames.class);

        // Failures are captured during the initialization because these tests are not async (the sources are immediate)

        try {
            initialize();
        } catch (Exception e) {
            Assertions.fail("Initialization not expected to fail", e);
            return;
        }

        Sink sink = get(Sink.class);
        await().until(() -> sink.listSink1().size() == 2
                && sink.listSink2().size() == 2
                && sink.listSink3().size() == 2);
        assertThat(sink.listSink1()).contains("abc", "def");
        assertThat(sink.listSink2()).contains("ABC", "DEF");
        assertThat(sink.listSink3()).contains("AbC", "DeF");
    }

    @Test
    void testSplitOfMessages() {
        addBeanClass(Sink.class, MessageSource.class, SplitterMultiOfMessages.class);

        // Failures are captured during the initialization because these tests are not async (the sources are immediate)

        try {
            initialize();
        } catch (Exception e) {
            Assertions.fail("Initialization not expected to fail", e);
            return;
        }

        Sink sink = get(Sink.class);
        await().until(() -> sink.listSink1().size() == 4);
        assertThat(sink.listSink1()).containsExactly("1", "2", "3", "4");
        assertThat(sink.listSink2()).containsExactly("1", "2");
        assertThat(sink.listSink3()).containsExactly("1", "2");
    }

    @ApplicationScoped
    public static class PayloadSource {
        @Outgoing("in")
        Multi<String> source() {
            return Multi.createFrom().items(
                    "abc", "ABC", "AbC", "DEF", "DeF", "def");
        }
    }

    public static class Key {
        public final String key;

        public Key(String key) {
            this.key = key;
        }

    }

    @ApplicationScoped
    public static class MessageSource {
        @Outgoing("in")
        Multi<Message<String>> source() {
            return Multi.createFrom().items(
                    Message.of("1").addMetadata(new Key("a")),
                    Message.of("1").addMetadata(new Key("b")),
                    Message.of("2").addMetadata(new Key("b")),
                    Message.of("2").addMetadata(new Key("a")),
                    Message.of("3").addMetadata(new Key("a")),
                    Message.of("1").addMetadata(new Key("c")),
                    Message.of("2").addMetadata(new Key("c")),
                    Message.of("4").addMetadata(new Key("a")));
        }
    }

    @ApplicationScoped
    public static class Sink {

        private final List<String> listSink1 = new CopyOnWriteArrayList<>();
        private final List<String> listSink2 = new CopyOnWriteArrayList<>();
        private final List<String> listSink3 = new CopyOnWriteArrayList<>();

        @Incoming("sink1")
        void all_caps(String s) {
            listSink1.add(s);
        }

        @Incoming("sink2")
        void all_low(String s) {
            listSink2.add(s);
        }

        @Incoming("sink3")
        void mixed(String s) {
            listSink3.add(s);
        }

        public List<String> listSink1() {
            return listSink1;
        }

        public List<String> listSink2() {
            return listSink2;
        }

        public List<String> listSink3() {
            return listSink3;
        }

    }

    @ApplicationScoped
    public static class SplitterMulti {

        enum Caps {
            ALL_CAPS,
            ALL_LOW,
            MIXED
        }

        @Incoming("in")
        @Outgoing("sink1")
        @Outgoing("sink2")
        @Outgoing("sink3")
        public MultiSplitter<String, Caps> reshape(Multi<String> in) {
            return in.split(Caps.class, s -> {
                if (Objects.equals(s, s.toLowerCase())) {
                    return Caps.ALL_LOW;
                } else if (Objects.equals(s, s.toUpperCase())) {
                    return Caps.ALL_CAPS;
                } else {
                    return Caps.MIXED;
                }
            });
        }

    }

    @ApplicationScoped
    public static class SplitterMultiWithChannelNames {

        enum Sinks {
            SINK1,
            SINK2,
            SINK3
        }

        @Incoming("in")
        @Outgoing("sink1")
        @Outgoing("sink2")
        @Outgoing("sink3")
        public MultiSplitter<String, Sinks> reshape(Multi<String> in) {
            return in.split(Sinks.class, s -> {
                if (Objects.equals(s, s.toLowerCase())) {
                    return Sinks.SINK1;
                } else if (Objects.equals(s, s.toUpperCase())) {
                    return Sinks.SINK2;
                } else {
                    return Sinks.SINK3;
                }
            });
        }

    }

    @ApplicationScoped
    public static class SplitterMultiOfMessages {

        enum MetadataKey {
            A,
            B,
            C;

            static MetadataKey fromKey(String key) {
                return MetadataKey.valueOf(key.toUpperCase());
            }
        }

        @Incoming("in")
        @Outgoing("sink1")
        @Outgoing("sink2")
        @Outgoing("sink3")
        public MultiSplitter<Message<String>, MetadataKey> reshape(Multi<Message<String>> in) {
            return in.split(MetadataKey.class, s -> {
                return MetadataKey.fromKey(s.getMetadata(Key.class).map(k -> k.key).orElse(null));
            });
        }

    }

}
