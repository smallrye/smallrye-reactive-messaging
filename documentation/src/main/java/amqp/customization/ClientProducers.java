package amqp.customization;

import jakarta.enterprise.inject.Produces;

import io.smallrye.common.annotation.Identifier;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;

public class ClientProducers {

    // <named>
    @Produces
    @Identifier("my-named-options")
    public AmqpClientOptions getNamedOptions() {
        // You can use the produced options to configure the TLS connection
        PemKeyCertOptions keycert = new PemKeyCertOptions()
                .addCertPath("./tls/tls.crt")
                .addKeyPath("./tls/tls.key");
        PemTrustOptions trust = new PemTrustOptions().addCertPath("./tlc/ca.crt");

        return new AmqpClientOptions()
                .setSsl(true)
                .setPemKeyCertOptions(keycert)
                .setPemTrustOptions(trust)
                .addEnabledSaslMechanism("EXTERNAL")
                .setHostnameVerificationAlgorithm("")
                .setConnectTimeout(30000)
                .setReconnectInterval(5000)
                .setContainerId("my-container");
    }
    // </named>

}
