package pulsar.configuration;

import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.inject.Produces;

import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.impl.AutoClusterFailover;
import org.apache.pulsar.client.impl.DefaultCryptoKeyReader;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.customroute.PartialRoundRobinMessageRouterImpl;

import io.smallrye.common.annotation.Identifier;

public class PulsarClientConfigProducers {

    // <consumer>
    @Produces
    @Identifier("my-consumer-options")
    public ConsumerConfigurationData<String> getConsumerConfig() {
        ConsumerConfigurationData<String> data = new ConsumerConfigurationData<>();
        data.setAckReceiptEnabled(true);
        data.setCryptoKeyReader(DefaultCryptoKeyReader.builder()
                //...
                .build());
        return data;
    }
    // </consumer>

    // <client>
    @Produces
    @Identifier("prices")
    public ClientConfigurationData getClientConfig() {
        ClientConfigurationData data = new ClientConfigurationData();
        data.setEnableTransaction(true);
        data.setServiceUrlProvider(AutoClusterFailover.builder()
                // ...
                .build());
        return data;
    }
    // </client>

    // <producer>
    @Produces
    @Identifier("prices")
    public Map<String, Object> getProducerconfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("batcherBuilder", BatcherBuilder.KEY_BASED);
        map.put("sendTimeoutMs", 3000);
        map.put("customMessageRouter", new PartialRoundRobinMessageRouterImpl(4));
        return map;
    }
    // </producer>
}
