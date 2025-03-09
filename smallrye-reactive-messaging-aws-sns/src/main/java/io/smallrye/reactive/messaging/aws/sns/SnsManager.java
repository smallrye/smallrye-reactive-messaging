package io.smallrye.reactive.messaging.aws.sns;

import static io.smallrye.reactive.messaging.aws.sns.i18n.AwsSnsLogging.log;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.utils.SdkAutoCloseable;

@ApplicationScoped
public class SnsManager {

    @Inject
    Instance<SnsClientProvider> clientProvider;

    private final Map<SnsClientConfig, SnsAsyncClient> clients = new ConcurrentHashMap<>();

    private final Map<SnsClientConfig, String> topicArns = new ConcurrentHashMap<>();

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        clients.entrySet().stream()
                .filter(e -> e.getKey().isComplete() && e.getValue() != null)
                .map(Map.Entry::getValue)
                .forEach(SdkAutoCloseable::close);
    }

    private SnsAsyncClient getClient(final SnsClientConfig config) {
        return clients.computeIfAbsent(config, c -> {
            if (clientProvider.isResolvable() && !clientProvider.isAmbiguous()) {
                return clientProvider.get().select(config.getTopicName());
            }
            try {
                final var builder = SnsAsyncClient.builder();
                if (c.getEndpointOverride() != null) {
                    builder.endpointOverride(URI.create(c.getEndpointOverride()));
                }
                if (c.getRegion() != null) {
                    builder.region(c.getRegion());
                }
                builder.credentialsProvider(config.createCredentialsProvider());
                return builder.build();
            } catch (final Exception e) {
                throw new DeploymentException("SnsAsyncClient creation failed.", e);
            }
        });
    }

    public SnsAsyncClient getClient(final SnsConnectorOutgoingConfiguration config) {
        return getClient(new SnsClientConfig(config));
    }

    public Uni<String> getTopicArn(final SnsConnectorOutgoingConfiguration config) {
        final var clientConfig = new SnsClientConfig(config);
        return Uni.createFrom().item(topicArns.computeIfAbsent(clientConfig, c -> {
            log.topicArnForChannel(config.getChannel(), clientConfig.getTopicArn());
            return clientConfig.getTopicArn();
        }));
    }
}
