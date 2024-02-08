package io.smallrye.reactive.messaging.aws.sqs;

import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.sqs.SqsClient;

@ApplicationScoped
public class SqsManager {

    public final Map<SqsConfig, SqsClient> clients = new HashMap<>();

    public SqsClient getClient(SqsConfig config) {
        return clients.computeIfAbsent(config, q -> {
            var builder = SqsClient
                    .builder();
            if (q.getEndpointOverride().isPresent()) {
                builder.endpointOverride(URI.create(q.getEndpointOverride().get()));
            }
            if (q.getRegion().isPresent()) {
                builder.region(q.getRegion().get());
            }
            return builder.build();
        });
    }
}
