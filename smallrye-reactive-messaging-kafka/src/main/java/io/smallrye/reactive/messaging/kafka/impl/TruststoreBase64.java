package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.config.SslConfigs;

/**
 * Turns the {@code ssl.truststore.base64} attribute into Kafka's native inline PEM truststore.
 * <p>
 * Kafka can only load JKS / PKCS12 trust stores from a file, but it accepts PEM certificates inline
 * ({@code ssl.truststore.type=PEM} and {@code ssl.truststore.certificates}). This lets the trust store be passed
 * as a base64 value (e.g. from an environment variable or a secret) without writing it to disk.
 */
public class TruststoreBase64 {

    public static final String SSL_TRUSTSTORE_BASE64 = "ssl.truststore.base64";

    private static final String PEM_TYPE = "PEM";
    private static final String PEM_HEADER = "-----BEGIN CERTIFICATE-----";
    private static final String PEM_FOOTER = "-----END CERTIFICATE-----";

    private TruststoreBase64() {
        // Avoid direct instantiation
    }

    /**
     * Replaces {@code ssl.truststore.base64}, if present, with {@code ssl.truststore.type=PEM} and
     * {@code ssl.truststore.certificates}. The attribute itself is always removed from the configuration.
     *
     * @param channel the channel name, used in error messages
     * @param config the Kafka client configuration, modified in place
     */
    public static void apply(String channel, Map<String, Object> config) {
        Object value = config.remove(SSL_TRUSTSTORE_BASE64);
        if (value == null) {
            return;
        }
        for (String conflicting : new String[] { SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG }) {
            if (config.containsKey(conflicting)) {
                throw ex.truststoreBase64Conflict(channel, conflicting);
            }
        }
        Object password = config.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, PEM_TYPE);
        config.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
                toPem(channel, value.toString(), password == null ? null : password.toString()));
    }

    static String toPem(String channel, String base64, String password) {
        byte[] content;
        try {
            content = Base64.getMimeDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw ex.truststoreBase64Invalid(channel, e);
        }
        String text = new String(content, StandardCharsets.US_ASCII);
        if (text.contains(PEM_HEADER)) {
            return text;
        }

        StringBuilder pem = new StringBuilder();
        try {
            // JDK key stores are dual-format: a JKS instance also loads PKCS12 content
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new ByteArrayInputStream(content), password == null ? null : password.toCharArray());
            Base64.Encoder encoder = Base64.getMimeEncoder(64, new byte[] { '\n' });
            for (String alias : Collections.list(keyStore.aliases())) {
                Certificate certificate = keyStore.getCertificate(alias);
                if (certificate != null) {
                    pem.append(PEM_HEADER).append('\n')
                            .append(encoder.encodeToString(certificate.getEncoded())).append('\n')
                            .append(PEM_FOOTER).append('\n');
                }
            }
        } catch (Exception e) {
            throw ex.truststoreBase64Invalid(channel, e);
        }
        if (pem.length() == 0) {
            throw ex.truststoreBase64NoCertificates(channel);
        }
        return pem.toString();
    }
}
