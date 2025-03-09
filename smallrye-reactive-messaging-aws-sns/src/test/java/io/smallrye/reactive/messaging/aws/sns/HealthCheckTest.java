package io.smallrye.reactive.messaging.aws.sns;

import static org.awaitility.Awaitility.await;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/**
 * We do not create the topic used during the tests. This creates a failure during the tests and should
 * reflect in the health check.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class HealthCheckTest extends SnsTestBase {

    @Test
    void should_not_be_alive_when_sending_messages_fails() {
        // given
        var config = initClientViaProvider();

        // when
        runApplication(config, ProducerApp.class);

        // then
        await().until(() -> !isAlive());
    }

    @Test
    void should_not_be_alive_when_sending_messages_via_batch_fails() {
        // given
        var config = initClientViaProvider()
                .with("mp.messaging.outgoing.sink.batch", true);

        // when
        runApplication(config, ProducerApp.class);

        // then
        await().until(() -> !isAlive());
    }

    @ApplicationScoped
    public static class ProducerApp {
        @Outgoing("sink")
        public Multi<PublishRequest.Builder> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> PublishRequest.builder()
                            .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                                    .dataType("String").stringValue("value").build()))
                            .message(String.valueOf(i)));
        }
    }

    private MapBasedConfig initClientViaProvider() {
        SnsTestClientProvider.client = getSnsClient();
        addBeans(SnsTestClientProvider.class);
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SnsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.topic", topic)
                .with("mp.messaging.outgoing.sink.topic.arn", topicArn);
    }
}
