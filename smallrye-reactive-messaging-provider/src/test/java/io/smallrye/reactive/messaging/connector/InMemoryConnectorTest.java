package io.smallrye.reactive.messaging.connector;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.MapBasedConfig;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class InMemoryConnectorTest extends WeldTestBaseWithoutTails {

    @Before
    public void install() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mp.messaging.incoming.foo.connector", InMemoryConnector.CONNECTOR);
        conf.put("mp.messaging.incoming.foo.data", "not read");
        conf.put("mp.messaging.outgoing.bar.connector", InMemoryConnector.CONNECTOR);
        conf.put("mp.messaging.outgoing.bar.data", "not read");
        installConfig(new MapBasedConfig(conf));
    }

    @After
    public void cleanup() {
        releaseConfig();
    }

    @Test
    public void testWithStrings() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        InMemorySink<String> bar = bean.sink("bar");
        InMemorySource<String> foo = bean.source("foo");
        foo.send("hello");
        assertThat(bar.received()).hasSize(1).extracting(Message::getPayload).containsExactly("HELLO");
    }

    @Test
    public void testWithMessages() {
        addBeanClass(MyBeanReceivingMessage.class);
        initialize();
        AtomicBoolean acked = new AtomicBoolean();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        InMemorySource<Message<String>> foo = bean.source("foo");
        InMemorySink<String> bar = bean.sink("bar");

        Message<String> msg = Message.of("hello", () -> {
            acked.set(true);
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return future;
        });
        foo.send(msg);
        assertThat(bar.received()).hasSize(1).extracting(Message::getPayload).containsExactly("HELLO");
        assertThat(acked).isTrue();
    }

    @Test
    public void testWithMultiplePayloads() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        InMemorySource<String> foo = bean.source("foo");
        InMemorySink<String> bar = bean.sink("bar");
        foo.send("1");
        foo.send("2");
        foo.send("3");
        assertThat(bar.received()).hasSize(3).extracting(Message::getPayload).containsExactly("1", "2", "3");
        bar.clear();
        foo.send("4");
        foo.send("5");
        foo.send("6");
        foo.complete();

        assertThat(bar.received()).hasSize(3).extracting(Message::getPayload).containsExactly("4", "5", "6");
        assertThat(bar.hasCompleted()).isTrue();
        assertThat(bar.hasFailed()).isFalse();
    }

    @Test
    public void testWithFailure() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        InMemorySource<String> foo = bean.source("foo");
        InMemorySink<String> bar = bean.sink("bar");
        foo.send("1");
        foo.send("2");
        foo.send("3");
        assertThat(bar.received()).hasSize(3).extracting(Message::getPayload).containsExactly("1", "2", "3");
        foo.fail(new Exception("boom"));

        assertThat(bar.hasCompleted()).isFalse();
        assertThat(bar.hasFailed()).isTrue();
        assertThat(bar.getFailure()).hasMessageContaining("boom");

        bar.clear();
        assertThat(bar.hasCompleted()).isFalse();
        assertThat(bar.hasFailed()).isFalse();
        assertThat(bar.getFailure()).isNull();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithUnknownSource() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        bean.source("unknown");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithUnknownSink() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        bean.sink("unknown");
    }

    @ApplicationScoped
    public static class MyBeanReceivingString {

        @Incoming("foo")
        @Outgoing("bar")
        public String process(String s) {
            return s.toUpperCase();
        }

    }

    @ApplicationScoped
    public static class MyBeanReceivingMessage {

        @Incoming("foo")
        @Outgoing("bar")
        public Message<String> process(Message<String> s) {
            return Message.of(s.getPayload().toUpperCase());
        }

    }

}
