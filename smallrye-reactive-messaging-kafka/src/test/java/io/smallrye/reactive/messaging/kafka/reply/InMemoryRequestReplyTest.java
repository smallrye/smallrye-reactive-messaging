package io.smallrye.reactive.messaging.kafka.reply;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.kafka.base.WeldTestBase;
import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySink;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class InMemoryRequestReplyTest extends WeldTestBase {

    @AfterEach
    void cleanup() {
        InMemoryConnector.clear();
    }

    @Test
    void testKafkaRequestReplyWithInMemoryConnector() {
        addBeans(InMemoryConnector.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.rr-out.connector", InMemoryConnector.CONNECTOR);

        RequestReplyApp application = runApplication(config, RequestReplyApp.class);

        KafkaRequestReply<String, String> rr = application.requestReply();
        assertThat(rr).isInstanceOf(NoOpKafkaRequestReplyImpl.class);

        ((NoOpKafkaRequestReplyImpl<String, String>) rr)
                .setReplyFunction(req -> "reply-to-" + req);

        String reply = rr.request("hello").await().indefinitely();
        assertThat(reply).isEqualTo("reply-to-hello");

        InMemoryConnector connector = getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        InMemorySink<String> sink = connector.sink("rr-out");
        assertThat(sink.received()).hasSize(1);
    }

    @Test
    void testReplyFunctionViaChannelRegistry() {
        addBeans(InMemoryConnector.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.rr-out.connector", InMemoryConnector.CONNECTOR);

        runApplication(config, RequestReplyApp.class);

        ChannelRegistry registry = get(ChannelRegistry.class);
        KafkaRequestReply<String, String> rr = registry.getEmitter("rr-out", KafkaRequestReply.class);

        assertThat(rr).isInstanceOf(NoOpKafkaRequestReplyImpl.class);
        ((NoOpKafkaRequestReplyImpl<String, String>) rr)
                .setReplyFunction(req -> "registry-reply-" + req);

        String reply = rr.request("world").await().indefinitely();
        assertThat(reply).isEqualTo("registry-reply-world");
    }

    @Test
    void testKafkaRequestReplyWaitForAssignments() {
        addBeans(InMemoryConnector.class);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.rr-out.connector", InMemoryConnector.CONNECTOR);

        RequestReplyApp application = runApplication(config, RequestReplyApp.class);

        assertThat(application.requestReply().waitForAssignments()
                .await().indefinitely()).isEmpty();
        assertThat(application.requestReply().getPendingReplies()).isEmpty();
        assertThat(application.requestReply().getConsumer()).isNull();
    }

    @ApplicationScoped
    public static class RequestReplyApp {

        @Inject
        @Channel("rr-out")
        KafkaRequestReply<String, String> requestReply;

        public KafkaRequestReply<String, String> requestReply() {
            return requestReply;
        }
    }
}
