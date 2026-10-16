package io.smallrye.reactive.messaging.kafka.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

class TruststoreBase64Test {

    private static final String SUBJECT = "CN=smallrye test CA";

    @Test
    void testJksTruststore() throws Exception {
        Map<String, Object> config = config("truststore.jks");
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeit");

        TruststoreBase64.apply("channel", config);

        assertConvertedToPem(config);
    }

    @Test
    void testJksTruststoreWithoutPassword() throws Exception {
        Map<String, Object> config = config("truststore.jks");

        TruststoreBase64.apply("channel", config);

        assertConvertedToPem(config);
    }

    @Test
    void testPkcs12Truststore() throws Exception {
        Map<String, Object> config = config("truststore.p12");
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeit");

        TruststoreBase64.apply("channel", config);

        assertConvertedToPem(config);
    }

    @Test
    void testPemCertificates() throws Exception {
        Map<String, Object> config = config("ca.pem");

        TruststoreBase64.apply("channel", config);

        assertConvertedToPem(config);
        assertThat(config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG)).isEqualTo(read("ca.pem"));
    }

    @Test
    void testUnsetLeavesConfigurationUntouched() {
        Map<String, Object> config = new HashMap<>();
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/truststore.jks");
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeit");

        TruststoreBase64.apply("channel", config);

        assertThat(config).containsOnly(
                Map.entry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/truststore.jks"),
                Map.entry(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeit"));
    }

    @Test
    void testConflictWithLocation() throws Exception {
        Map<String, Object> config = config("truststore.jks");
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/truststore.jks");

        assertThatThrownBy(() -> TruststoreBase64.apply("my-channel", config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-channel")
                .hasMessageContaining(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    }

    @Test
    void testConflictWithCertificates() throws Exception {
        Map<String, Object> config = config("truststore.jks");
        config.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, read("ca.pem"));

        assertThatThrownBy(() -> TruststoreBase64.apply("my-channel", config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG);
    }

    @Test
    void testWrongPassword() throws Exception {
        Map<String, Object> config = config("truststore.p12");
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "wrong");

        assertThatThrownBy(() -> TruststoreBase64.apply("my-channel", config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unable to read the trust store");
    }

    @Test
    void testNotBase64() {
        Map<String, Object> config = new HashMap<>();
        config.put(TruststoreBase64.SSL_TRUSTSTORE_BASE64, "not base64 !");

        assertThatThrownBy(() -> TruststoreBase64.apply("my-channel", config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unable to read the trust store");
    }

    @Test
    void testAppliedToProducerConfiguration() throws Exception {
        MapBasedConfig channelConfig = new MapBasedConfig()
                .with("channel-name", "my-channel")
                .with("topic", "my-topic")
                .with("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .with("security.protocol", "SSL")
                .with(TruststoreBase64.SSL_TRUSTSTORE_BASE64, base64("truststore.jks"));

        Map<String, Object> config = ReactiveKafkaProducer.getKafkaProducerConfiguration(
                new KafkaConnectorOutgoingConfiguration(channelConfig), UnsatisfiedInstance.instance());

        assertConvertedToPem(config);
        // The Kafka client loads the trust store when it is created, before connecting to any broker
        new KafkaProducer<>(config).close(Duration.ZERO);
    }

    private static void assertConvertedToPem(Map<String, Object> config) throws Exception {
        assertThat(config).doesNotContainKeys(TruststoreBase64.SSL_TRUSTSTORE_BASE64,
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(config).containsEntry(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");

        String pem = (String) config.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG);
        X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
        assertThat(certificate.getSubjectX500Principal().getName()).isEqualTo(SUBJECT);
    }

    private static Map<String, Object> config(String resource) throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put(TruststoreBase64.SSL_TRUSTSTORE_BASE64, base64(resource));
        return config;
    }

    private static String base64(String resource) throws IOException {
        return Base64.getEncoder().encodeToString(bytes(resource));
    }

    private static String read(String resource) throws IOException {
        return new String(bytes(resource), StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(String resource) throws IOException {
        try (InputStream in = TruststoreBase64Test.class.getResourceAsStream("/truststore-base64/" + resource)) {
            return in.readAllBytes();
        }
    }
}
