package io.smallrye.reactive.messaging.mqtt.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.mqtt.MqttConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

class MqttHelpersTest {

    private MqttConnectorCommonConfiguration createConfig(Map<String, Object> values) {
        if (!values.containsKey("channel-name")) {
            values.put("channel-name", "test-channel");
        }
        if (!values.containsKey("host")) {
            values.put("host", "localhost");
        }
        return new MqttConnectorCommonConfiguration(new MapBasedConfig(values));
    }

    @AfterEach
    void tearDown() {
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Nested
    class CreateClientOptionsTests {

        @Test
        void basicConfigValues() {
            Map<String, Object> config = new HashMap<>();
            config.put("host", "mqtt.example.com");
            config.put("port", 1883);
            config.put("username", "user1");
            config.put("password", "secret");
            config.put("client-id", "my-client");
            config.put("auto-clean-session", true);
            config.put("keep-alive-seconds", 60);
            config.put("max-message-size", 16384);
            config.put("max-inflight-queue", 20);
            config.put("connect-timeout-seconds", 30);

            MqttClientSessionOptions options = MqttHelpers.createClientOptions(
                    createConfig(config), null);

            assertThat(options.getHostname()).isEqualTo("mqtt.example.com");
            assertThat(options.getPort()).isEqualTo(1883);
            assertThat(options.getUsername()).isEqualTo("user1");
            assertThat(options.getPassword()).isEqualTo("secret");
            assertThat(options.getClientId()).isEqualTo("my-client");
            assertThat(options.isCleanSession()).isTrue();
            assertThat(options.getKeepAliveInterval()).isEqualTo(60);
            assertThat(options.getMaxMessageSize()).isEqualTo(16384);
            assertThat(options.getMaxInflightQueue()).isEqualTo(20);
            assertThat(options.getConnectTimeout()).isEqualTo(30000);
        }

        @Test
        void defaultPortWhenSslDisabled() {
            Map<String, Object> config = new HashMap<>();
            config.put("ssl", false);

            MqttClientSessionOptions options = MqttHelpers.createClientOptions(
                    createConfig(config), null);

            assertThat(options.getPort()).isEqualTo(1883);
        }

        @Test
        void defaultPortWhenSslEnabled() {
            Map<String, Object> config = new HashMap<>();
            config.put("ssl", true);

            MqttClientSessionOptions options = MqttHelpers.createClientOptions(
                    createConfig(config), null);

            assertThat(options.getPort()).isEqualTo(8883);
        }

        @Test
        void sslHostnameVerificationNoneDisablesVerification() {
            Map<String, Object> config = new HashMap<>();
            config.put("ssl.hostname-verification-algorithm", "NONE");

            MqttClientSessionOptions options = MqttHelpers.createClientOptions(
                    createConfig(config), null);

            assertThat(options.getHostnameVerificationAlgorithm()).isEmpty();
        }

        @Test
        void sslHostnameVerificationHttpsAlgorithm() {
            Map<String, Object> config = new HashMap<>();
            config.put("ssl.hostname-verification-algorithm", "HTTPS");

            MqttClientSessionOptions options = MqttHelpers.createClientOptions(
                    createConfig(config), null);

            assertThat(options.getHostnameVerificationAlgorithm()).isEqualTo("HTTPS");
        }

        @Test
        void metricsNameContainsChannelName() {
            Map<String, Object> config = new HashMap<>();
            config.put("channel-name", "my-mqtt-channel");

            MqttClientSessionOptions options = MqttHelpers.createClientOptions(
                    createConfig(config), null);

            assertThat(options.getMetricsName()).isEqualTo("mqtt|my-mqtt-channel");
        }
    }

    @Nested
    class RebuildMatchesWithSharedSubscriptionTests {

        @Test
        void regularTopicUnchanged() {
            assertThat(MqttHelpers.rebuildMatchesWithSharedSubscription("test/topic"))
                    .isEqualTo("test/topic");
        }

        @Test
        void sharedSubscriptionPrefixRemoved() {
            assertThat(MqttHelpers.rebuildMatchesWithSharedSubscription("$share/group1/test/topic"))
                    .isEqualTo("test/topic");
        }

        @Test
        void sharedSubscriptionWithWildcard() {
            assertThat(MqttHelpers.rebuildMatchesWithSharedSubscription("$share/mygroup/sensor/#"))
                    .isEqualTo("sensor/#");
        }

        @Test
        void sharedSubscriptionSingleLevel() {
            assertThat(MqttHelpers.rebuildMatchesWithSharedSubscription("$share/g/topic"))
                    .isEqualTo("topic");
        }

        @Test
        void dollarSignNotShared() {
            assertThat(MqttHelpers.rebuildMatchesWithSharedSubscription("$SYS/broker/load"))
                    .isEqualTo("$SYS/broker/load");
        }

        @Test
        void sharedSubscriptionWithMultipleLevels() {
            assertThat(MqttHelpers.rebuildMatchesWithSharedSubscription("$share/consumer-group/a/b/c/d"))
                    .isEqualTo("a/b/c/d");
        }
    }
}
