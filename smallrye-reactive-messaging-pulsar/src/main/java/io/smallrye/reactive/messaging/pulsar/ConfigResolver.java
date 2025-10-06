package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.providers.helpers.CDIUtils.getInstanceById;
import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.eclipse.microprofile.config.Config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;

import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.providers.helpers.ConfigUtils;
import io.smallrye.reactive.messaging.providers.helpers.Validation;
import io.vertx.core.json.JsonObject;

/**
 * Precedence of config resolution, the least priority to the highest, each step overriding the previous one.
 * <p>
 * 1. Map&lt;String, Object&gt; config map produced with default config identifier
 * 2. Map&lt;String, Object&gt; config map produced with identifier in the configuration or channel name
 * 3. ConfigurationData object produced with identifier in the configuration or channel name
 * 4. Channel configuration properties named with ConfigurationData field names
 */
@ApplicationScoped
public class ConfigResolver {

    public static final String DEFAULT_PULSAR_CLIENT = "default-pulsar-client";

    public static final String DEFAULT_PULSAR_CONSUMER = "default-pulsar-consumer";

    public static final String DEFAULT_PULSAR_PRODUCER = "default-pulsar-producer";

    public static final TypeReference<Map<String, Object>> OBJECT_MAP_TYPE_REF = new TypeReference<>() {
    };

    private final Instance<Map<String, Object>> configurations;
    private final Instance<ClientConfigurationData> clientConfigurations;
    private final Instance<ConsumerConfigurationData<?>> consumerConfigurations;
    private final Instance<ProducerConfigurationData> producerConfigurations;

    private final ObjectMapper mapper;
    private final Instance<ClientCustomizer<ClientBuilder>> clientConfigCustomizers;
    private final Instance<ClientCustomizer<ConsumerBuilder<?>>> consumerConfigCustomizers;
    private final Instance<ClientCustomizer<ProducerBuilder<?>>> producerConfigCustomizers;

    @Inject
    public ConfigResolver(@Any Instance<Map<String, Object>> configurations,
            @Any Instance<ClientConfigurationData> clientConfigurations,
            @Any Instance<ClientCustomizer<ClientBuilder>> clientConfigCustomizers,
            @Any Instance<ConsumerConfigurationData<?>> consumerConfigurations,
            @Any Instance<ClientCustomizer<ConsumerBuilder<?>>> consumerConfigCustomizers,
            @Any Instance<ProducerConfigurationData> producerConfigurations,
            @Any Instance<ClientCustomizer<ProducerBuilder<?>>> producerConfigCustomizers) {
        this.configurations = configurations;
        this.clientConfigurations = clientConfigurations;
        this.clientConfigCustomizers = clientConfigCustomizers;
        this.consumerConfigurations = consumerConfigurations;
        this.consumerConfigCustomizers = consumerConfigCustomizers;
        this.producerConfigurations = producerConfigurations;
        this.producerConfigCustomizers = producerConfigCustomizers;
        this.mapper = new ObjectMapper();
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, false);
        this.mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        this.mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public Map<String, Object> configToMap(Object loadedConfig) {
        if (loadedConfig == null) {
            return new HashMap<>();
        }
        return new HashMap<>(mapper.convertValue(loadedConfig, OBJECT_MAP_TYPE_REF));
    }

    /**
     * Extract the configuration map for building Pulsar client
     *
     * @param cc the pulsar incoming or outgoing channel configuration
     * @return the config map loadable by a Pulsar client
     */
    public ClientConfigurationData getClientConf(PulsarConnectorCommonConfiguration cc) {
        Map<String, Object> defaultConfig = getInstanceById(configurations, DEFAULT_PULSAR_CLIENT, HashMap::new);
        Map<String, Object> channelConfig = getInstanceById(configurations,
                cc.getClientConfiguration().orElse(cc.getChannel()), HashMap::new);
        ClientConfigurationData conf = getInstanceById(clientConfigurations,
                cc.getClientConfiguration().orElse(cc.getChannel()), ClientConfigurationData::new);
        return mergeConfig(conf.clone(), mergeMap(defaultConfig, channelConfig), cc.config());
    }

    public ClientBuilderImpl customize(ClientBuilderImpl builder, PulsarConnectorCommonConfiguration cc) {
        return (ClientBuilderImpl) ConfigUtils.customize(cc.config(), clientConfigCustomizers, builder);
    }

    public ClientBuilderImpl configure(PulsarConnectorCommonConfiguration cc, ClientConfigurationData conf)
            throws PulsarClientException {
        setAuth(conf);
        return customize(new ClientBuilderImpl(conf), cc);
    }

    /**
     * Sets the authentication object in the given configuration object using
     * `authPluginClassName` and `authParams`/`authParamMap` attributes
     * This use to be done by the PulsarClientImpl
     *
     * @param conf client configuration
     * @throws PulsarClientException
     */
    private void setAuth(ClientConfigurationData conf) throws PulsarClientException {
        if (Validation.isBlank(conf.getAuthPluginClassName())
                || (Validation.isBlank(conf.getAuthParams()) && conf.getAuthParamMap() == null)) {
            return;
        }

        if (!Validation.isBlank(conf.getAuthParams())) {
            conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParams()));
        } else if (conf.getAuthParamMap() != null) {
            conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParamMap()));
        }
    }

    /**
     * Extract the configuration map for building Pulsar consumer
     *
     * @param ic the pulsar incoming channel configuration
     * @return the config map loadable by a Pulsar consumer
     */
    public ConsumerConfigurationData<?> getConsumerConf(PulsarConnectorIncomingConfiguration ic) {
        Map<String, Object> defaultConfig = getInstanceById(configurations, DEFAULT_PULSAR_CONSUMER, HashMap::new);
        Map<String, Object> channelConfig = getInstanceById(configurations,
                ic.getConsumerConfiguration().orElse(ic.getChannel()), HashMap::new);
        ConsumerConfigurationData<?> conf = getInstanceById(consumerConfigurations,
                ic.getConsumerConfiguration().orElse(ic.getChannel()), ConsumerConfigurationData::new);
        return mergeConfig(conf.clone(), mergeMap(defaultConfig, channelConfig), ic.config());
    }

    public <T> ConsumerBuilder<T> customize(ConsumerBuilder<T> builder, PulsarConnectorIncomingConfiguration ic) {
        return (ConsumerBuilder<T>) ConfigUtils.customize(ic.config(), consumerConfigCustomizers, builder);
    }

    public <T> ConsumerBuilder<T> configure(ConsumerBuilder<T> builder,
            PulsarConnectorIncomingConfiguration ic,
            ConsumerConfigurationData<?> conf) {
        builder.loadConf(configToMap(conf));
        ic.getDeadLetterPolicyMaxRedeliverCount().ifPresent(i -> builder.deadLetterPolicy(getDeadLetterPolicy(ic, i)));
        ic.getNegativeAckRedeliveryBackoff()
                .ifPresent(s -> builder.negativeAckRedeliveryBackoff(parseBackoff(s, ic.getChannel())));
        ic.getAckTimeoutRedeliveryBackoff()
                .ifPresent(s -> builder.ackTimeoutRedeliveryBackoff(parseBackoff(s, ic.getChannel())));
        if (conf.getConsumerEventListener() != null) {
            builder.consumerEventListener(conf.getConsumerEventListener());
        }
        if (conf.getPayloadProcessor() != null) {
            builder.messagePayloadProcessor(conf.getPayloadProcessor());
        }
        if (conf.getKeySharedPolicy() != null) {
            builder.keySharedPolicy(conf.getKeySharedPolicy());
        } else if (conf.getSubscriptionType() == SubscriptionType.Key_Shared) {
            builder.keySharedPolicy(KeySharedPolicy.autoSplitHashRange());
        }
        if (conf.getCryptoKeyReader() != null) {
            builder.cryptoKeyReader(conf.getCryptoKeyReader());
        }
        if (conf.getMessageCrypto() != null) {
            builder.messageCrypto(conf.getMessageCrypto());
        }
        if (ic.getBatchReceive()) {
            builder.batchReceivePolicy(
                    Objects.requireNonNullElse(conf.getBatchReceivePolicy(), BatchReceivePolicy.DEFAULT_POLICY));
        }
        return customize(builder, ic);
    }

    private static DeadLetterPolicy getDeadLetterPolicy(PulsarConnectorIncomingConfiguration ic, Integer redeliverCount) {
        return DeadLetterPolicy.builder()
                .maxRedeliverCount(redeliverCount)
                .deadLetterTopic(ic.getDeadLetterPolicyDeadLetterTopic().orElse(null))
                .retryLetterTopic(ic.getDeadLetterPolicyRetryLetterTopic().orElse(null))
                .initialSubscriptionName(ic.getDeadLetterPolicyInitialSubscriptionName().orElse(null))
                .build();
    }

    private RedeliveryBackoff parseBackoff(String backoffString, String channel) {
        String[] strings = backoffString.split(",");
        try {
            return MultiplierRedeliveryBackoff.builder()
                    .minDelayMs(Long.parseLong(strings[0]))
                    .maxDelayMs(Long.parseLong(strings[1]))
                    .multiplier(Double.parseDouble(strings[2]))
                    .build();
        } catch (Exception e) {
            log.unableToParseRedeliveryBackoff(backoffString, channel);
            return null;
        }
    }

    /**
     * Extract the configuration map for building Pulsar producer
     *
     * @param oc the pulsar outgoing channel configuration
     * @return the config map loadable by a Pulsar producer
     */
    public ProducerConfigurationData getProducerConf(PulsarConnectorOutgoingConfiguration oc) {
        Map<String, Object> defaultConfig = getInstanceById(configurations, DEFAULT_PULSAR_PRODUCER, HashMap::new);
        Map<String, Object> channelConfig = getInstanceById(configurations,
                oc.getProducerConfiguration().orElse(oc.getChannel()), HashMap::new);
        ProducerConfigurationData conf = getInstanceById(producerConfigurations,
                oc.getProducerConfiguration().orElse(oc.getChannel()), ProducerConfigurationData::new);
        return mergeConfig(conf.clone(), mergeMap(defaultConfig, channelConfig), oc.config());
    }

    public <T> ProducerBuilder<T> customize(ProducerBuilder<T> builder, PulsarConnectorOutgoingConfiguration oc) {
        return (ProducerBuilder<T>) ConfigUtils.customize(oc.config(), producerConfigCustomizers, builder);
    }

    public <T> ProducerBuilder<T> configure(ProducerBuilder<T> builder,
            PulsarConnectorOutgoingConfiguration oc,
            ProducerConfigurationData conf) {
        builder.loadConf(configToMap(conf));
        if (conf.getCustomMessageRouter() != null) {
            builder.messageRouter(conf.getCustomMessageRouter());
        }
        if (conf.getBatcherBuilder() != null) {
            builder.batcherBuilder(conf.getBatcherBuilder());
        }
        if (conf.getCryptoKeyReader() != null) {
            builder.cryptoKeyReader(conf.getCryptoKeyReader());
        }
        for (String encryptionKey : conf.getEncryptionKeys()) {
            builder.addEncryptionKey(encryptionKey);
        }
        return customize(builder, oc);
    }

    private Map<String, Object> mergeMap(Map<String, Object> defaultConfig, Map<String, Object> channelConfig) {
        Map<String, Object> map = new HashMap<>(defaultConfig);
        map.putAll(channelConfig);
        return map;
    }

    private <T> T mergeConfig(T conf, Map<String, Object> mapConf, Config config) {
        try {
            Map<String, Object> map = asJsonObject(config, new HashMap<>(mapConf)).getMap();
            return mapper.updateValue(conf, map);
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        }
    }

    public static JsonObject asJsonObject(Config config, Map<String, Object> confToOverride) {
        JsonObject json = new JsonObject(confToOverride);
        Iterable<String> propertyNames = config.getPropertyNames();
        for (String originalKey : propertyNames) {
            extractConfigKey(config, json, originalKey, "");
        }
        return json;
    }

    private static void extractConfigKey(Config config, JsonObject json, String originalKey, String prefixToStrip) {
        // Transform keys that may come from environment variables.
        // As pulsar properties use lowerCamelCase instead of UPPER_UNDERSCORE
        String key = originalKey;
        if (key.contains("_") || allCaps(key)) {
            key = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, originalKey);
        }
        String jsonKey = key.substring(prefixToStrip.length());
        try {
            Optional<Integer> i = config.getOptionalValue(key, Integer.class);
            if (!i.isPresent()) {
                i = config.getOptionalValue(originalKey, Integer.class);
            }

            if (i.isPresent() && i.get() instanceof Integer) {
                json.put(jsonKey, i.get());
                return;
            }
        } catch (ClassCastException | IllegalArgumentException e) {
            // Ignore me
        }

        try {
            Optional<Double> d = config.getOptionalValue(key, Double.class);
            if (!d.isPresent()) {
                d = config.getOptionalValue(originalKey, Double.class);
            }
            if (d.isPresent() && d.get() instanceof Double) {
                json.put(jsonKey, d.get());
                return;
            }
        } catch (ClassCastException | IllegalArgumentException e) {
            // Ignore me
        }

        try {
            String s = config.getOptionalValue(key, String.class)
                    .orElseGet(() -> config.getOptionalValue(originalKey, String.class).orElse(null));
            if (s != null) {
                String value = s.trim();
                if (value.equalsIgnoreCase("false")) {
                    json.put(jsonKey, false);
                } else if (value.equalsIgnoreCase("true")) {
                    json.put(jsonKey, true);
                } else {
                    json.put(jsonKey, value);
                }
                return;
            }
        } catch (ClassCastException e) {
            // Ignore me
        }

        // We need to do boolean last, as it would return `false` for any non-parsable object.
        try {
            Optional<Boolean> d = config.getOptionalValue(key, Boolean.class);
            if (!d.isPresent()) {
                d = config.getOptionalValue(originalKey, Boolean.class);
            }
            if (d.isPresent()) {
                json.put(jsonKey, d.get());
            }
        } catch (ClassCastException | IllegalArgumentException e) {
            // Ignore the entry
        }
    }

    private static boolean allCaps(String key) {
        return key.toUpperCase().equals(key);
    }

    public ConsumerConfigurationData<?> getConfig(ConsumerBuilder<?> consumerBuilder) {
        return ((ConsumerBuilderImpl<?>) consumerBuilder).getConf();
    }

    public ProducerConfigurationData getConfig(ProducerBuilder<?> producerBuilder) {
        return ((ProducerBuilderImpl<?>) producerBuilder).getConf();
    }

}
