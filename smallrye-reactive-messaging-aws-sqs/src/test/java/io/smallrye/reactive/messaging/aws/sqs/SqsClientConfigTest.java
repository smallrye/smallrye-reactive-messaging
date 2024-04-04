package io.smallrye.reactive.messaging.aws.sqs;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SqsClientConfigTest extends SqsTestBase {

    @Test
    void testRegionConfigMissing() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue);

        Assertions.assertThatThrownBy(() -> runApplication(config, ConsumerApp.class))
                .hasMessageContaining("The required configuration property \"region\" is missing");
    }

    @Test
    void testRegionConfigNotFound() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.region", "unknown-region");

        runApplication(config, ConsumerApp.class);
    }

    @Test
    void testConfig() {
        createQueue(queue);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.endpoint-override", localstack.getEndpoint().toString())
                .with("mp.messaging.incoming.data.region", localstack.getRegion())
                .with("mp.messaging.incoming.data.credentials-provider",
                        "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider");

        runApplication(config, ConsumerApp.class);
    }

    @Test
    void testConfigCredentialsProvider() {
        createQueue(queue);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.endpoint-override", localstack.getEndpoint().toString())
                .with("mp.messaging.incoming.data.region", localstack.getRegion())
                .with("mp.messaging.incoming.data.credentials-provider", "not_existing_provider");

        runApplication(config, ConsumerApp.class);
    }

    @ApplicationScoped
    public static class ConsumerApp {

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(String msg) {
            received.add(msg);
        }

        public List<String> received() {
            return received;
        }
    }
}
