package io.smallrye.reactive.messaging.kafka.companion.test;

import static io.smallrye.reactive.messaging.kafka.companion.test.EmbeddedKafkaBroker.endpoint;
import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.TestTags;
import kafka.server.KafkaConfig;

@Tag(TestTags.FLAKY)
public class EmbeddedKafkaTest {

    @Test
    void test() {
        try (EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker().start()) {
            String advertisedListeners = broker.getAdvertisedListeners();
            assertThat(advertisedListeners).contains("PLAINTEXT").doesNotContain("CONTROLLER");
            KafkaCompanion companion = new KafkaCompanion(advertisedListeners);
            assertThat(companion.topics().list()).isEmpty();
            assertFunctionalBroker(companion);
            companion.close();
        }
    }

    @Test
    void testWithExistingLogDir(@TempDir File dir) {
        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker()
                .withNodeId(0)
                .withAdditionalProperties(props -> props.put(KafkaConfig.LogDirProp(), dir.toPath().toString()))
                .withDeleteLogDirsOnClose(false);

        // format storage before starting
        EmbeddedKafkaBroker.formatStorage(Collections.singletonList(dir.toPath().toString()),
                broker.getClusterId(), broker.getNodeId(), false);

        broker.start();

        KafkaCompanion companion = new KafkaCompanion(broker.getAdvertisedListeners());
        assertFunctionalBroker(companion);
        assertThat(broker.getLogDirs()).containsExactly(dir.toString());

        companion.close();
        broker.close();
    }

    @Test
    void testSsl() {
        Endpoint external = endpoint("EXTERNAL", SSL, "localhost", 0);
        Endpoint internal = endpoint("INTERNAL", PLAINTEXT, "localhost", 0);
        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker()
                .withAdvertisedListeners(external, internal)
                .withAdditionalProperties(properties -> {
                    properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                            Paths.get("src/test/resources/server.keystore.p12").toAbsolutePath().toString());
                    properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "serverks");
                    properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "serverks");
                    properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
                    properties.put(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, "SHA1PRNG");
                });
        broker.start();

        KafkaCompanion companion = new KafkaCompanion(EmbeddedKafkaBroker.toListenerString(external));
        Map<String, Object> common = new HashMap<>();
        common.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        common.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                Paths.get("src/test/resources/client.truststore.p12").toAbsolutePath().toString());
        common.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "clientts");
        common.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        companion.setCommonClientConfig(common);
        assertFunctionalBroker(companion);
        companion.close();
        broker.close();
    }

    void assertFunctionalBroker(KafkaCompanion companion) {
        // create topic and wait
        companion.topics().createAndWait("messages", 3);
        // produce messages
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>("messages", i % 3, "k", "" + i), 100);
        // consume messages
        companion.consumeStrings().fromTopics("messages", 100).awaitCompletion();
    }
}
