package io.smallrye.reactive.messaging.providers.connectors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.memory.TestIncoming;
import io.smallrye.reactive.messaging.memory.TestOutgoing;
import io.smallrye.reactive.messaging.memory.TestingConnector;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class TestingConnectorTest extends WeldTestBase {

    @BeforeEach
    public void install() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mp.messaging.incoming.foo.connector", TestingConnector.CONNECTOR);
        conf.put("mp.messaging.outgoing.bar.connector", TestingConnector.CONNECTOR);
        installConfig(new MapBasedConfig(conf));
    }

    @AfterEach
    public void cleanup() {
        releaseConfig();
    }

    @Test
    public void testWithStrings() {
        addBeanClass(StringProcessor.class);
        initialize();

        TestingConnector connector = container.getBeanManager().createInstance()
                .select(TestingConnector.class, ConnectorLiteral.of(TestingConnector.CONNECTOR)).get();
        TestOutgoing<String> bar = connector.outgoing("bar");
        TestIncoming<String> foo = connector.incoming("foo");
        foo.deliver("hello");
        assertThat(bar.sent()).hasSize(1).extracting(Message::getPayload).containsExactly("HELLO");
    }

    @Test
    public void testSwitchMethods() {
        Map<String, String> props = TestingConnector.switchIncomingChannelsToTesting("in1", "in2");
        assertThat(props).containsEntry("mp.messaging.incoming.in1.connector", "smallrye-testing");
        assertThat(props).containsEntry("mp.messaging.incoming.in2.connector", "smallrye-testing");
        assertThat(System.getProperty("mp.messaging.incoming.in1.connector")).isEqualTo("smallrye-testing");

        Map<String, String> outProps = TestingConnector.switchOutgoingChannelsToTesting("out1");
        assertThat(outProps).containsEntry("mp.messaging.outgoing.out1.connector", "smallrye-testing");

        TestingConnector.clear();
        assertThat(System.getProperty("mp.messaging.incoming.in1.connector")).isNull();
        assertThat(System.getProperty("mp.messaging.incoming.in2.connector")).isNull();
        assertThat(System.getProperty("mp.messaging.outgoing.out1.connector")).isNull();
    }

    @Test
    public void testDeliverWithMetadata() {
        addBeanClass(MetadataReader.class);
        initialize();

        TestingConnector connector = container.getBeanManager().createInstance()
                .select(TestingConnector.class, ConnectorLiteral.of(TestingConnector.CONNECTOR)).get();
        TestIncoming<String> foo = connector.incoming("foo");

        MyTestMetadata meta = new SimpleTestMetadata("test-value");
        foo.deliver("hello", meta);

        MetadataReader reader = container.getBeanManager().createInstance().select(MetadataReader.class).get();
        assertThat(reader.getPayload()).isEqualTo("hello");
        assertThat(reader.getMetadata()).isNotNull();
        assertThat(reader.getMetadata().getValue()).isEqualTo("test-value");
    }

    public interface MyTestMetadata {
        String getValue();
    }

    public static class SimpleTestMetadata implements MyTestMetadata {
        private final String value;

        public SimpleTestMetadata(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    @ApplicationScoped
    public static class StringProcessor {

        @Incoming("foo")
        @Outgoing("bar")
        public String process(String s) {
            return s.toUpperCase();
        }
    }

    @ApplicationScoped
    public static class MetadataReader {

        private final AtomicReference<String> payload = new AtomicReference<>();
        private final AtomicReference<MyTestMetadata> metadata = new AtomicReference<>();

        @Incoming("foo")
        public CompletionStage<Void> process(Message<String> msg) {
            payload.set(msg.getPayload());
            msg.getMetadata(MyTestMetadata.class).ifPresent(metadata::set);
            return msg.ack();
        }

        public String getPayload() {
            return payload.get();
        }

        public MyTestMetadata getMetadata() {
            return metadata.get();
        }
    }
}
