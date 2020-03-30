package customization;

import io.vertx.amqp.AmqpClientOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;

import javax.enterprise.inject.Produces;
import javax.inject.Named;

public class ClientProducers {

    // tag::named[]
    @Produces
    @Named("my-named-options")
    public AmqpClientOptions getNamedOptions() {
        // You can use the produced options to configure the TLS connection
        PemKeyCertOptions keycert = new PemKeyCertOptions()
            .addCertPath("./tls/tls.crt")
            .addKeyPath("./tls/tls.key");
        PemTrustOptions trust =
            new PemTrustOptions().addCertPath("./tlc/ca.crt");

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
    // end::named[]

}
