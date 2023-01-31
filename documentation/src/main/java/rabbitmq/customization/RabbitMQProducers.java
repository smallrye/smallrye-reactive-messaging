package rabbitmq.customization;

import jakarta.enterprise.inject.Produces;

import io.smallrye.common.annotation.Identifier;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.rabbitmq.RabbitMQOptions;

public class RabbitMQProducers {

    // <named>
    @Produces
    @Identifier("my-named-options")
    public RabbitMQOptions getNamedOptions() {
        // You can use the produced options to configure the TLS connection
        PemKeyCertOptions keycert = new PemKeyCertOptions()
                .addCertPath("./tls/tls.crt")
                .addKeyPath("./tls/tls.key");
        PemTrustOptions trust = new PemTrustOptions().addCertPath("./tlc/ca.crt");

        return (RabbitMQOptions) new RabbitMQOptions()
                .setUser("admin")
                .setPassword("test")
                .setSsl(true)
                .setPemKeyCertOptions(keycert)
                .setPemTrustOptions(trust)
                .setHostnameVerificationAlgorithm("")
                .setConnectTimeout(30000)
                .setReconnectInterval(5000);
    }
    // </named>

}
