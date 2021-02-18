package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;
import static java.lang.String.format;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.TrustManagerFactory;

import com.hivemq.client.mqtt.MqttClientSslConfigBuilder;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.Mqtt3ClientBuilder;
import com.hivemq.client.mqtt.mqtt3.Mqtt3RxClient;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.converters.uni.UniRxConverters;
import io.smallrye.reactive.messaging.health.HealthReport;

public class Clients {

    private static final Map<String, ClientHolder> clients = new ConcurrentHashMap<>();

    private Clients() {
        // avoid direct instantiation.
    }

    static Uni<Mqtt3RxClient> getConnectedClient(MqttConnectorCommonConfiguration options) {

        ClientHolder holder = getHolder(options);

        return holder.connect();
    }

    static ClientHolder getHolder(MqttConnectorCommonConfiguration options) {

        String host = options.getHost();
        int def = options.getSsl() ? 8883 : 1883;
        int port = options.getPort().orElse(def);
        String server = options.getServerName().orElse("");
        String clientId = options.getClientId().orElse("");

        String id = host + ":" + port + "<" + server + ">-[" + clientId + "]";

        return clients.computeIfAbsent(id, key -> new ClientHolder(options));
    }

    static Mqtt3RxClient create(MqttConnectorCommonConfiguration options) {

        final Mqtt3ClientBuilder builder = Mqtt3Client.builder()
                .serverHost(options.getHost())
                .serverPort(options.getPort().orElse(options.getSsl() ? 8883 : 1883));

        if (options.getAutoGeneratedClientId()) {
            builder.identifier(UUID.randomUUID().toString());
        }
        options.getClientId().ifPresent(clientid -> builder.identifier(clientid));

        options.getUsername().ifPresent(username -> {
            builder.simpleAuth()
                    .username(username)
                    .password(options.getPassword().orElseThrow(
                            () -> new IllegalArgumentException("password null with authentication enabled (username not null)"))
                            .getBytes())
                    .applySimpleAuth();
        });

        if (options.getSsl()) {
            final MqttClientSslConfigBuilder.Nested<? extends Mqtt3ClientBuilder> nested = builder.sslConfig();
            options.getCaCartFile()
                    .ifPresent(file -> nested.trustManagerFactory(createSelfSignedTrustManagerFactory(file)));
            nested.applySslConfig();
        }

        return builder
                .automaticReconnectWithDefaultConfig()
                .addConnectedListener(context -> {
                    log.info(format("connected to %s:%d", context.getClientConfig().getServerHost(),
                            context.getClientConfig().getServerPort()));
                }).buildRx();
    }

    /**
     * Removed all the stored clients.
     */
    public static void clear() {
        clients.forEach((name, holder) -> holder.close());
        clients.clear();
    }

    public static void checkLiveness(HealthReport.HealthReportBuilder builder) {
        clients.forEach((name, holder) -> builder.add(name, holder.checkLiveness()));
    }

    public static void checkReadiness(HealthReport.HealthReportBuilder builder) {
        clients.forEach((name, holder) -> builder.add(name, holder.checkReadiness()));
    }

    public static class ClientHolder {

        private final Mqtt3RxClient client;
        private final Uni<Mqtt3ConnAck> connection;
        private final int livenessTimeout;
        private final int readinessTimeout;

        private long lastMqttUpdate = 0;

        public ClientHolder(MqttConnectorCommonConfiguration options) {
            client = create(options);

            livenessTimeout = options.getLivenessTimeout();
            readinessTimeout = options.getReadinessTimeout();

            client.toAsync().subscribeWith()
                    .topicFilter("$SYS/broker/uptime")
                    .callback(m -> {
                        log.debug(new String(m.getPayloadAsBytes()));
                        lastMqttUpdate = System.currentTimeMillis();
                    })
                    .send();

            connection = Uni.createFrom().converter(UniRxConverters.fromSingle(), client.connect()).memoize()
                    .indefinitely();

        }

        public Uni<Mqtt3RxClient> connect() {
            return connection
                    .map(ignored -> client);
        }

        public boolean checkLiveness() {
            return (System.currentTimeMillis() - lastMqttUpdate) < livenessTimeout;
        }

        public boolean checkReadiness() {
            return (System.currentTimeMillis() - lastMqttUpdate) < readinessTimeout;
        }

        public void close() {
            final Mqtt3BlockingClient mqtt3BlockingClient = client.toBlocking();
            if (mqtt3BlockingClient.getState().isConnected()) {
                mqtt3BlockingClient.disconnect();
            }
        }
    }

    public static TrustManagerFactory createSelfSignedTrustManagerFactory(String selfSignedTrustManager) {
        try {
            // Add support for self-signed (local) SSL certificates
            // Based on http://developer.android.com/training/articles/security-ssl.html#UnknownCa

            // Load CAs from an InputStream
            // (could be from a resource or ByteArrayInputStream or ...)
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            // From https://www.washington.edu/itconnect/security/ca/load-der.crt
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(selfSignedTrustManager);
            Certificate ca;
            try (InputStream caInput = new BufferedInputStream(is)) {
                ca = cf.generateCertificate(caInput);
            }

            // Create a KeyStore containing our trusted CAs
            String keyStoreType = KeyStore.getDefaultType();
            KeyStore keyStore = KeyStore.getInstance(keyStoreType);
            keyStore.load(null, null);
            keyStore.setCertificateEntry("ca", ca);

            // Create a TrustManager that trusts the CAs in our KeyStore
            String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
            tmf.init(keyStore);

            return tmf;
        } catch (CertificateException | IOException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
