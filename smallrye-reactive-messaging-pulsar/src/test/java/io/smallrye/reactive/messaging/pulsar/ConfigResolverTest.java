package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.DefaultCryptoKeyReader;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.pulsar.base.SingletonInstance;
import io.smallrye.reactive.messaging.pulsar.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

class ConfigResolverTest {

    @Test
    void emptyConsumerConfig() {
        ConfigResolver configResolver = new ConfigResolver(
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        ConsumerConfigurationData<?> conf = configResolver
                .getConsumerConf(new PulsarConnectorIncomingConfiguration(getChannelConfig()));
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsKeys(
                "ackReceiptEnabled",
                "ackTimeoutMillis",
                "acknowledgementsGroupTimeMicros",
                "autoAckOldestChunkedMessageOnQueueFull",
                "autoUpdatePartitions",
                "autoUpdatePartitionsIntervalSeconds",
                "batchIndexAckEnabled",
                "cryptoFailureAction",
                "expireTimeOfIncompleteChunkedMessageMillis",
                "maxPendingChuckedMessage",
                "maxPendingChunkedMessage",
                "maxTotalReceiverQueueSizeAcrossPartitions",
                "negativeAckRedeliveryDelayMicros",
                "patternAutoDiscoveryPeriod",
                "poolMessages",
                "priorityLevel",
                "readCompacted",
                "receiverQueueSize",
                "regexSubscriptionMode",
                "replicateSubscriptionState",
                "resetIncludeHead",
                "retryEnable",
                "startPaused",
                "subscriptionInitialPosition",
                "subscriptionMode",
                "subscriptionType",
                "tickDurationMillis");
    }

    @Test
    void mapConsumerConfig() {
        ConfigResolver configResolver = new ConfigResolver(
                new SingletonInstance<>("my-consumer-config", Map.of("topicNames", Arrays.asList("t1", "t2"))),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(getChannelConfig()
                .with("consumer-configuration", "my-consumer-config"));
        ConsumerConfigurationData<?> conf = configResolver.getConsumerConf(ic);
        assertThat(conf.getTopicNames()).containsExactly("t1", "t2");
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).hasEntrySatisfying("topicNames", v -> {
            assertThat(v).asList().containsExactly("t1", "t2");
        });
    }

    private static MapBasedConfig getChannelConfig() {
        return new MapBasedConfig().with("channel-name", "channel");
    }

    @Test
    void configDefaultConsumerConfig() {
        ConfigResolver configResolver = new ConfigResolver(
                new SingletonInstance<>("default-pulsar-consumer", Map.of("topicNames", Arrays.asList("t1", "t2"))),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(getChannelConfig());
        ConsumerConfigurationData<?> conf = configResolver.getConsumerConf(ic);
        assertThat(conf.getTopicNames()).containsExactly("t1", "t2");
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).hasEntrySatisfying("topicNames", v -> {
            assertThat(v).asList().containsExactly("t1", "t2");
        });
    }

    @Test
    void configOverrideConsumerConfig() {
        ConsumerConfigurationData<Object> data = new ConsumerConfigurationData<>();
        data.setAckReceiptEnabled(true);
        data.setTopicNames(Set.of("t3"));
        data.setSubscriptionName("my-subscription");
        data.setConsumerEventListener(new ConsumerEventListener() {
            @Override
            public void becameActive(Consumer<?> consumer, int partitionId) {

            }

            @Override
            public void becameInactive(Consumer<?> consumer, int partitionId) {

            }
        });
        ConfigResolver configResolver = new ConfigResolver(
                new SingletonInstance<>("my-consumer-config", Map.of("topicNames", Arrays.asList("t1", "t2"))),
                UnsatisfiedInstance.instance(),
                new SingletonInstance<>("my-consumer-config", data),
                UnsatisfiedInstance.instance());

        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(getChannelConfig()
                .with("consumer-configuration", "my-consumer-config")
                .with("subscriptionName", "other-subscription"));
        ConsumerConfigurationData<?> conf = configResolver.getConsumerConf(ic);

        assertThat(conf.getTopicNames()).containsExactly("t1", "t2");
        assertThat(conf.getSubscriptionName()).isEqualTo("other-subscription");
        assertThat(conf.isAckReceiptEnabled()).isTrue();
        assertThat(conf.getConsumerEventListener()).isNotNull();
        assertThat(conf).isNotEqualTo(data);

        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsKeys("subscriptionName", "topicNames", "ackReceiptEnabled");
    }

    @Test
    void configOverrideConsumerConfigWithChannelName() {
        ConsumerConfigurationData<Object> data = new ConsumerConfigurationData<>();
        data.setAckReceiptEnabled(true);
        data.setTopicNames(Set.of("t1", "t2"));
        data.setSubscriptionName("my-subscription");
        data.setConsumerEventListener(new ConsumerEventListener() {
            @Override
            public void becameActive(Consumer<?> consumer, int partitionId) {

            }

            @Override
            public void becameInactive(Consumer<?> consumer, int partitionId) {

            }
        });
        ConfigResolver configResolver = new ConfigResolver(
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                new SingletonInstance<>("channel", data),
                UnsatisfiedInstance.instance());

        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(getChannelConfig()
                .with("subscriptionName", "other-subscription"));
        ConsumerConfigurationData<?> conf = configResolver.getConsumerConf(ic);

        assertThat(conf.getTopicNames()).containsExactly("t1", "t2");
        assertThat(conf.getSubscriptionName()).isEqualTo("other-subscription");
        assertThat(conf.isAckReceiptEnabled()).isTrue();
        assertThat(conf.getConsumerEventListener()).isNotNull();
        assertThat(conf).isNotEqualTo(data);

        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsKeys("subscriptionName", "topicNames", "ackReceiptEnabled");
    }

    @Test
    void emptyClientConfig() {
        ConfigResolver configResolver = new ConfigResolver(
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        ClientConfigurationData conf = configResolver
                .getClientConf(new PulsarConnectorCommonConfiguration(getChannelConfig()));
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsKeys(
                "concurrentLookupRequest",
                "connectionTimeoutMs",
                "connectionsPerBroker",
                "dnsLookupBindPort",
                "enableBusyWait",
                "enableTransaction",
                "initialBackoffIntervalNanos",
                "keepAliveIntervalSeconds",
                "lookupTimeoutMs",
                "maxBackoffIntervalNanos",
                "maxLookupRedirects",
                "maxLookupRequest",
                "maxNumberOfRejectedRequestPerConnection",
                "memoryLimitBytes",
                "numIoThreads",
                "numListenerThreads",
                "operationTimeoutMs",
                "requestTimeoutMs",
                "statsIntervalSeconds",
                "tlsAllowInsecureConnection",
                "tlsHostnameVerificationEnable",
                "tlsTrustStoreType",
                "useKeyStoreTls",
                "useTcpNoDelay",
                "useTls");
    }

    @Test
    void mapClientConfig() {
        ConfigResolver configResolver = new ConfigResolver(
                new SingletonInstance<>("my-client-config", Map.of("serviceUrl", "pulsar://localhost")),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        PulsarConnectorCommonConfiguration cc = new PulsarConnectorCommonConfiguration(getChannelConfig()
                .with("client-configuration", "my-client-config"));
        ClientConfigurationData conf = configResolver.getClientConf(cc);
        assertThat(conf.getServiceUrl()).isEqualTo("pulsar://localhost");
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsEntry("serviceUrl", "pulsar://localhost");
    }

    @Test
    void configDefaultClientConfig() {
        ConfigResolver configResolver = new ConfigResolver(
                new SingletonInstance<>("default-pulsar-client", Map.of("serviceUrl", "pulsar://localhost")),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        PulsarConnectorCommonConfiguration cc = new PulsarConnectorCommonConfiguration(getChannelConfig());
        ClientConfigurationData conf = configResolver.getClientConf(cc);
        assertThat(conf.getServiceUrl()).isEqualTo("pulsar://localhost");
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsEntry("serviceUrl", "pulsar://localhost");
    }

    @Test
    void configOverrideClientConfig() {
        ClientConfigurationData data = new ClientConfigurationData();
        data.setAuthParams("params");
        data.setServiceUrlProvider(new ServiceUrlProvider() {
            @Override
            public void initialize(PulsarClient client) {

            }

            @Override
            public String getServiceUrl() {
                return null;
            }
        });
        ConfigResolver configResolver = new ConfigResolver(
                new SingletonInstance<>("my-client-config", Map.of("serviceUrl", "pulsar://localhost")),
                new SingletonInstance<>("my-client-config", data),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        PulsarConnectorCommonConfiguration cc = new PulsarConnectorCommonConfiguration(getChannelConfig()
                .with("client-configuration", "my-client-config")
                .with("connectionTimeoutMs", "5000"));
        ClientConfigurationData conf = configResolver.getClientConf(cc);
        assertThat(conf.getServiceUrl()).isEqualTo("pulsar://localhost");
        assertThat(conf.getServiceUrlProvider()).isNotNull();
        assertThat(conf.getAuthParams()).isEqualTo("params");
        assertThat(conf.getConnectionTimeoutMs()).isEqualTo(5000);
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsEntry("serviceUrl", "pulsar://localhost")
                .containsKeys("authParams", "connectionTimeoutMs");
    }

    @Test
    void emptyProducerConfig() {
        ConfigResolver configResolver = new ConfigResolver(
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        ProducerConfigurationData conf = configResolver
                .getProducerConf(new PulsarConnectorOutgoingConfiguration(getChannelConfig()));
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsKeys(
                "accessMode",
                "autoUpdatePartitions",
                "autoUpdatePartitionsIntervalSeconds",
                "batchingEnabled",
                "batchingMaxBytes",
                "batchingMaxMessages",
                "batchingMaxPublishDelayMicros",
                "batchingPartitionSwitchFrequencyByPublishDelay",
                "blockIfQueueFull",
                "chunkingEnabled",
                "compressionType",
                "cryptoFailureAction",
                "hashingScheme",
                "lazyStartPartitionedProducers",
                "maxPendingMessages",
                "maxPendingMessagesAcrossPartitions",
                "multiSchema",
                "properties",
                "sendTimeoutMs");
    }

    @Test
    void mapProducerConfig() {
        ConfigResolver configResolver = new ConfigResolver(
                new SingletonInstance<>("my-producer-config", Map.of("topicName", "t1")),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(getChannelConfig()
                .with("producer-configuration", "my-producer-config"));
        ProducerConfigurationData conf = configResolver.getProducerConf(oc);
        assertThat(conf.getTopicName()).isEqualTo("t1");
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsEntry("topicName", "t1");
    }

    @Test
    void configDefaultProducerConfig() {
        ConfigResolver configResolver = new ConfigResolver(
                new SingletonInstance<>("default-pulsar-producer", Map.of("topicName", "t1")),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance());

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(getChannelConfig());
        ProducerConfigurationData conf = configResolver.getProducerConf(oc);
        assertThat(conf.getTopicName()).isEqualTo("t1");
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsEntry("topicName", "t1");
    }

    @Test
    void configOverrideProducerConfig() {
        ProducerConfigurationData data = new ProducerConfigurationData();
        data.setBatchingEnabled(true);
        data.setBatchingMaxMessages(1000);
        data.setTopicName("t2");
        data.setSendTimeoutMs(1000L);
        data.setCryptoKeyReader(DefaultCryptoKeyReader.builder().build());

        ConfigResolver configResolver = new ConfigResolver(
                new SingletonInstance<>("my-producer-config", Map.of("topicName", "t1")),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                new SingletonInstance<>("my-producer-config", data));

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(getChannelConfig()
                .with("producer-configuration", "my-producer-config")
                .with("sendTimeoutMs", 2000));
        ProducerConfigurationData conf = configResolver.getProducerConf(oc);
        assertThat(conf.getTopicName()).isEqualTo("t1");
        assertThat(conf.isBatchingEnabled()).isTrue();
        assertThat(conf.getBatchingMaxMessages()).isEqualTo(1000);
        assertThat(conf.getSendTimeoutMs()).isEqualTo(2000L);
        assertThat(conf.getCryptoKeyReader()).isNotNull();
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsEntry("topicName", "t1")
                .containsEntry("sendTimeoutMs", 2000L)
                .containsEntry("batchingEnabled", true);
    }

    @Test
    void configOverrideProducerConfigWithChannelName() {
        ProducerConfigurationData data = new ProducerConfigurationData();
        data.setBatchingEnabled(true);
        data.setBatchingMaxMessages(1000);
        data.setTopicName("t1");
        data.setSendTimeoutMs(1000L);
        data.setCryptoKeyReader(DefaultCryptoKeyReader.builder().build());

        ConfigResolver configResolver = new ConfigResolver(
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                UnsatisfiedInstance.instance(),
                new SingletonInstance<>("channel", data));

        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(getChannelConfig()
                .with("sendTimeoutMs", 2000));
        ProducerConfigurationData conf = configResolver.getProducerConf(oc);
        assertThat(conf.getTopicName()).isEqualTo("t1");
        assertThat(conf.isBatchingEnabled()).isTrue();
        assertThat(conf.getBatchingMaxMessages()).isEqualTo(1000);
        assertThat(conf.getSendTimeoutMs()).isEqualTo(2000L);
        assertThat(conf.getCryptoKeyReader()).isNotNull();
        Map<String, Object> map = configResolver.configToMap(conf);
        assertThat(map).containsEntry("topicName", "t1")
                .containsEntry("sendTimeoutMs", 2000L)
                .containsEntry("batchingEnabled", true);
    }
}
