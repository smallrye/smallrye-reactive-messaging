package io.smallrye.reactive.messaging.wiring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.*;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connectors.MyDummyConnector;

@SetSystemProperty(key = "smallrye.messaging.wiring.auto-wire-to-connector", value = "true")
public class ConnectorDetectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testIncomingWithNoConnector() {

        addBeanClass(AppWithIncoming.class);

        initialize();
        Wiring wiring = get(Wiring.class);
        assertThat(wiring.getGraph().isClosed()).isFalse();
    }

    @Test
    public void testIncomingWithSingleConnector() {

        addBeanClass(MyConnector.class);
        addBeanClass(AppWithIncoming.class);

        initialize();

        AppWithIncoming incoming = get(AppWithIncoming.class);
        assertThat(incoming.sink()).containsExactly("a", "b", "c");
    }

    @Test
    public void testIncomingWithTwoConnectors() {

        addBeanClass(AppWithIncoming.class);
        addBeanClass(MyConnector.class);
        addBeanClass(MyDummyConnector.class);

        initialize();
        Wiring wiring = get(Wiring.class);
        assertThat(wiring.getGraph().isClosed()).isFalse();
    }

    @Test
    public void testTwoIncomingWithSingleConnector() {

        addBeanClass(MyConnector.class);
        addBeanClass(AppWithIncoming.class);
        addBeanClass(SecondAppWithIncoming.class);

        initialize();

        AppWithIncoming incoming = get(AppWithIncoming.class);
        assertThat(incoming.sink()).containsExactly("a", "b", "c");

        SecondAppWithIncoming incoming2 = get(SecondAppWithIncoming.class);
        assertThat(incoming2.sink()).containsExactly("a", "b", "c");
    }

    @Test
    public void testOutgoingWithSingleConnector() {

        addBeanClass(MyConnector.class);
        addBeanClass(AppWithOutgoing.class);

        initialize();

        MyConnector connector = get(MyConnector.class, ConnectorLiteral.of("my-connector"));
        assertThat(connector.sink()).containsExactly("a", "b", "c");
    }

    @Test
    public void testOutgoingWithNoConnector() {

        addBeanClass(AppWithOutgoing.class);

        initialize();
        Wiring wiring = get(Wiring.class);
        assertThat(wiring.getGraph().isClosed()).isFalse();
    }

    @Test
    public void testOutgoingWithTwoConnectors() {

        addBeanClass(AppWithOutgoing.class);
        addBeanClass(MyConnector.class);
        addBeanClass(MyDummyConnector.class);

        initialize();
        Wiring wiring = get(Wiring.class);
        assertThat(wiring.getGraph().isClosed()).isFalse();
    }

    @Test
    public void testTwoOutgoingWithSingleConnector() {

        addBeanClass(MyConnector.class);
        addBeanClass(AppWithOutgoing.class);
        addBeanClass(SecondAppWithOutgoing.class);

        initialize();

        MyConnector connector = get(MyConnector.class, ConnectorLiteral.of("my-connector"));
        assertThat(connector.sink()).containsExactlyInAnyOrder("a", "b", "c", "d", "e", "f");
    }

    @Test
    public void testBothIncomingAndOutgoing() {

        addBeanClass(MyConnector.class);
        addBeanClass(AppWithIncomingAndOutgoing.class);

        initialize();

        AppWithIncomingAndOutgoing app = get(AppWithIncomingAndOutgoing.class);
        assertThat(app.sink()).containsExactly("a", "b", "c");

        MyConnector connector = get(MyConnector.class, ConnectorLiteral.of("my-connector"));
        assertThat(connector.sink()).containsExactlyInAnyOrder("d", "e", "f");
    }

    @Test
    public void testWithChannelInjection() {

        addBeanClass(MyConnector.class);
        addBeanClass(AppWithChannelInjection.class);

        initialize();

        AppWithChannelInjection incoming = get(AppWithChannelInjection.class);
        assertThat(incoming.consume()).containsExactly("a", "b", "c");
    }

    @Test
    public void testWithEmitterInjection() {

        addBeanClass(MyConnector.class);
        addBeanClass(AppWithEmitter.class);

        initialize();

        MyConnector connector = get(MyConnector.class, ConnectorLiteral.of("my-connector"));
        get(AppWithEmitter.class).emit();
        await().until(() -> connector.sink().size() == 3);
        assertThat(connector.sink()).containsExactly("a", "b", "c");
    }

    @ApplicationScoped
    public static class AppWithIncoming {

        private final List<String> sink = new ArrayList<>();

        @Incoming("foo")
        public void consume(String m) {
            sink.add(m);
        }

        public List<String> sink() {
            return sink;
        }

    }

    @ApplicationScoped
    public static class SecondAppWithIncoming {

        private final List<String> sink = new ArrayList<>();

        @Incoming("foo2")
        public void consume(String m) {
            sink.add(m);
        }

        public List<String> sink() {
            return sink;
        }

    }

    @ApplicationScoped
    public static class AppWithOutgoing {
        @Outgoing("foo")
        public Multi<String> source() {
            return Multi.createFrom().items("a", "b", "c");
        }
    }

    @ApplicationScoped
    public static class SecondAppWithOutgoing {
        @Outgoing("foo2")
        public Multi<String> source() {
            return Multi.createFrom().items("d", "e", "f");
        }
    }

    @ApplicationScoped
    @Connector("my-connector")
    public static class MyConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

        private final List<String> sink = new ArrayList<>();

        @Override
        public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
            return ReactiveStreams.fromPublisher(Multi.createFrom().items("a", "b", "c")
                    .map(Message::of));
        }

        @Override
        public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
            return ReactiveStreams.<Message<?>> builder()
                    .forEach(m -> sink.add(m.getPayload().toString()));
        }

        public List<String> sink() {
            return sink;
        }
    }

    @ApplicationScoped
    public static class AppWithIncomingAndOutgoing {

        private final List<String> sink = new ArrayList<>();

        @Incoming("foo")
        public void consume(String m) {
            sink.add(m);
        }

        @Outgoing("bar")
        public Multi<String> source() {
            return Multi.createFrom().items("d", "e", "f");
        }

        public List<String> sink() {
            return sink;
        }

    }

    @ApplicationScoped
    public static class AppWithChannelInjection {

        @Inject
        @Channel("foo")
        Multi<String> stream;

        public List<String> consume() {
            return stream.collect().asList()
                    .await().indefinitely();
        }

    }

    @ApplicationScoped
    public static class AppWithEmitter {

        @Inject
        @Channel("foo")
        Emitter<String> emitter;

        public void emit() {
            emitter.send("a");
            emitter.send("b");
            emitter.send("c");
        }
    }

}
