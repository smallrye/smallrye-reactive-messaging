package io.smallrye.reactive.messaging.mqtt.hivemq;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.UUID;

import javax.net.ssl.TrustManagerFactory;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.Mqtt3ClientBuilder;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;

import io.reactivex.Flowable;

public class NotificationService {

    private Mqtt3BlockingClient client;

    private final String brokerHost;
    private final int brokerPort;
    private final String brokerUsername;
    private final String brokerPassword;
    private final boolean useSSL;
    private final boolean useAuth;

    public NotificationService(String brokerHost, int brokerPort,
            String brokerUsername, String brokerPassword,
            boolean useSSL, boolean useAuth) {
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.brokerUsername = brokerUsername;
        this.brokerPassword = brokerPassword;
        this.useSSL = useSSL;
        this.useAuth = useAuth;
    }

    void configureBroker() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        final Mqtt3ClientBuilder mqtt3ClientBuilder = Mqtt3Client.builder().identifier(UUID.randomUUID().toString())
                .serverHost(brokerHost)
                .serverPort(brokerPort)
                .automaticReconnectWithDefaultConfig()
                .addConnectedListener(context -> {
                });

        if (useAuth) {
            mqtt3ClientBuilder.simpleAuth().username(brokerUsername).password(brokerPassword.getBytes()).applySimpleAuth();
        }

        if (useSSL) {
            mqtt3ClientBuilder.sslConfig().trustManagerFactory(createSelfSignedTrustManagerFactory()).applySslConfig();
        }

        client = mqtt3ClientBuilder
                .buildBlocking();

        final Flowable<Mqtt3Publish> mqtt3PublishFlowable = client.toRx().subscribePublishesWith()
                .topicFilter("test/topic")
                .qos(MqttQos.EXACTLY_ONCE)
                .applySubscribe()
                .doOnSingle(subAck -> System.out.println("subscribed"))
                .doOnNext(publish -> System.out.println("received publish"));

        client.toAsync().subscribeWith()
                .topicFilter("prespe-clients/deviceStatus")
                .callback(this::updateStatus)
                .send();
    }

    private void updateStatus(Mqtt3Publish mqtt5Publish) {
        mqtt5Publish.getPayloadAsBytes();
    }

    public static TrustManagerFactory createSelfSignedTrustManagerFactory()
            throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
        // Add support for self-signed (local) SSL certificates
        // Based on http://developer.android.com/training/articles/security-ssl.html#UnknownCa

        // Load CAs from an InputStream
        // (could be from a resource or ByteArrayInputStream or ...)
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        // From https://www.washington.edu/itconnect/security/ca/load-der.crt
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("tls/esselunga_ca.crt");
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
    }

    public void connectToBroker() {
        client.connect();
    }

    void publish(String deviceId, final byte[] payloadAsJsonBytes) {
        final String topicNameSpace = String.format("prespe-clients/%s/messaggi", deviceId);

        client.publishWith().topic(topicNameSpace).qos(MqttQos.EXACTLY_ONCE).payload(payloadAsJsonBytes).send();
    }

}
