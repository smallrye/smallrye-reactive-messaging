package rabbitmq.og.customization;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import jakarta.enterprise.inject.Produces;

import com.rabbitmq.client.ConnectionFactory;

import io.smallrye.common.annotation.Identifier;

public class RabbitMQProducers {

    // <named>
    @Produces
    @Identifier("my-named-options")
    public ConnectionFactory getNamedOptions() throws NoSuchAlgorithmException, KeyManagementException {
        // You can use the produced ConnectionFactory to configure the connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("admin");
        factory.setPassword("test");
        factory.useSslProtocol();
        factory.setConnectionTimeout(30000);
        factory.setNetworkRecoveryInterval(5000);
        return factory;
    }
    // </named>

}
