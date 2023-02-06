package mqtt.customization;

import jakarta.enterprise.inject.Produces;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;

public class ClientProducers {

    // <named>
    @Produces
    @Identifier("my-options")
    public MqttClientSessionOptions getOptions() {
        // You can use the produced options to configure the TLS connection
        PemKeyCertOptions keycert = new PemKeyCertOptions()
                .addCertPath("./tls/tls.crt")
                .addKeyPath("./tls/tls.key");
        PemTrustOptions trust = new PemTrustOptions().addCertPath("./tlc/ca.crt");

        return new MqttClientSessionOptions()
                .setSsl(true)
                .setPemKeyCertOptions(keycert)
                .setPemTrustOptions(trust)
                .setHostnameVerificationAlgorithm("")
                .setConnectTimeout(30000)
                .setReconnectInterval(5000);
    }
    // </named>

}
