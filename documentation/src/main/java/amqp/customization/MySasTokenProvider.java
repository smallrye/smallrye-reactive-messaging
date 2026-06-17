package amqp.customization;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.amqp.cbs.CbsToken;
import io.smallrye.reactive.messaging.amqp.cbs.CbsTokenProvider;
import io.smallrye.reactive.messaging.amqp.cbs.DefaultCbsToken;

// <sas-provider>
@ApplicationScoped
public class MySasTokenProvider implements CbsTokenProvider {

    @Override
    public Uni<CbsToken> getToken(AmqpConnectorCommonConfiguration config) {
        String sasToken = generateSasToken(config.getHost());
        return Uni.createFrom().item(new DefaultCbsToken(
                sasToken,
                "servicebus.windows.net:sastoken",
                3600)); // valid for 1 hour
    }

    private String generateSasToken(String host) {
        // Build a SAS token for Azure Service Bus
        return "SharedAccessSignature sr=...";
    }
}
// </sas-provider>
