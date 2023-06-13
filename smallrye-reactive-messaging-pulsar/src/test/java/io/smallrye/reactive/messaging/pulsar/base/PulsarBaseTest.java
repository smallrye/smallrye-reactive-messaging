package io.smallrye.reactive.messaging.pulsar.base;

import java.util.concurrent.Executors;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationBasic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(PulsarBrokerExtension.class)
public class PulsarBaseTest extends PulsarClientBaseTest {

    @BeforeAll
    static void init(@PulsarBrokerExtension.PulsarServiceUrl String clusterUrl,
            @PulsarBrokerExtension.PulsarServiceHttpUrl String serviceHttpUrl)
            throws PulsarClientException {
        serviceUrl = clusterUrl;
        httpUrl = serviceHttpUrl;
        AuthenticationBasic auth = new AuthenticationBasic();
        auth.configure("{\"userId\":\"superuser\",\"password\":\"admin\"}");
        client = PulsarClient.builder()
                .serviceUrl(clusterUrl)
                .authentication(auth)
                .enableTransaction(true)
                .build();
        admin = PulsarAdmin.builder()
                .authentication(auth)
                .serviceHttpUrl(serviceHttpUrl)
                .build();
        executor = Executors.newFixedThreadPool(1);
    }

}
