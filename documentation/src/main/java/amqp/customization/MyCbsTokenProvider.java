package amqp.customization;

import java.time.OffsetDateTime;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.amqp.AmqpConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.amqp.cbs.CbsToken;
import io.smallrye.reactive.messaging.amqp.cbs.CbsTokenProvider;
import io.smallrye.reactive.messaging.amqp.cbs.DefaultCbsToken;

// <provider>
@ApplicationScoped
public class MyCbsTokenProvider implements CbsTokenProvider {

    @Override
    public Uni<CbsToken> getToken(AmqpConnectorCommonConfiguration config) {
        String token = fetchTokenFromAuthService();
        return Uni.createFrom().item(new DefaultCbsToken(
                token,
                "jwt",
                OffsetDateTime.now().plusHours(1),
                OffsetDateTime.now().plusMinutes(45)));
    }

    private String fetchTokenFromAuthService() {
        // Retrieve a token from your identity provider
        return "my-jwt-token";
    }
}
// </provider>
