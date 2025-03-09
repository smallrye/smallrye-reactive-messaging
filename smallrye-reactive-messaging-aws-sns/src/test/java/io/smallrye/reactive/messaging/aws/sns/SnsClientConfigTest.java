package io.smallrye.reactive.messaging.aws.sns;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;

import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.core.exception.SdkClientException;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SnsClientConfigTest extends SnsTestBase {

    @Test
    void should_fail_client_creation_if_region_is_missing() {
        // given
        var config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SnsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.topic", topic)
                .with("mp.messaging.outgoing.data.topic.arn", topicArn);

        // when / then
        Assertions.assertThatThrownBy(() -> runApplication(config, ProducerApp.class))
                .hasMessageContaining("SnsAsyncClient creation failed")
                .hasRootCauseInstanceOf(SdkClientException.class);
    }

    @Test
    void should_create_client_with_unknown_region() {
        // given
        var config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SnsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.topic", topic)
                .with("mp.messaging.outgoing.data.topic.arn", topicArn)
                .with("mp.messaging.outgoing.data.region", "unknown-region");

        // when / then
        runApplication(config, ProducerApp.class);
    }

    @Test
    void should_create_client_with_endpoint_override() {
        // given
        var subscription = createTopicWithQueueSubscription(topic);
        var config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SnsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.topic", topic)
                .with("mp.messaging.outgoing.data.topic.arn", topicArn)
                .with("mp.messaging.outgoing.data.region", localstack.getRegion())
                .with("mp.messaging.outgoing.data.endpoint-override", localstack.getEndpoint().toString());

        // when
        runApplication(config, ProducerApp.class);

        // then
        verifyTestMessage(subscription);
    }

    @Test
    void should_create_client_with_credentials_provider() {
        // given
        var subscription = createTopicWithQueueSubscription(topic);
        var config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SnsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.topic", topic)
                .with("mp.messaging.outgoing.data.topic.arn", topicArn)
                .with("mp.messaging.outgoing.data.region", localstack.getRegion())
                .with("mp.messaging.outgoing.data.endpoint-override", localstack.getEndpoint().toString())
                .with("mp.messaging.outgoing.data.credentials-provider",
                        "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider");

        // when
        runApplication(config, ProducerApp.class);

        // then
        verifyTestMessage(subscription);
    }

    @Test
    void should_create_client_with_fallback_when_providing_invalid_credentials_provider() {
        // given
        var subscription = createTopicWithQueueSubscription(topic);
        var config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SnsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.topic", topic)
                .with("mp.messaging.outgoing.data.topic.arn", topicArn)
                .with("mp.messaging.outgoing.data.region", localstack.getRegion())
                .with("mp.messaging.outgoing.data.endpoint-override", localstack.getEndpoint().toString())
                .with("mp.messaging.outgoing.data.credentials-provider", "not_existing_provider");

        // when
        runApplication(config, ProducerApp.class);

        // then
        verifyTestMessage(subscription);
    }

    private void verifyTestMessage(final TopicQueueSubscription subscription) {
        var messages = receiveAndDeleteMessages(subscription.queueUrl(), 1, Duration.ofSeconds(10));
        assertThat(messages.get(0).body()).isEqualTo("test");
    }

    @ApplicationScoped
    public static class ProducerApp {
        @Outgoing("data")
        public Multi<String> produce() {
            return Multi.createFrom().item("test");
        }
    }
}
