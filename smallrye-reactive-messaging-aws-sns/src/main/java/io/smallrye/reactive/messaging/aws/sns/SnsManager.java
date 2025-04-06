package io.smallrye.reactive.messaging.aws.sns;

import static io.smallrye.reactive.messaging.aws.sns.i18n.AwsSnsLogging.log;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.inject.Inject;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.utils.SdkAutoCloseable;

@ApplicationScoped
public class SnsManager {

    @Inject
    @Any
    Instance<SnsAsyncClient> providedClients;

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
            final Optional<SnsAsyncClient> injectedClient = getInjectedClient(config);
            if (injectedClient.isPresent()) {
                return injectedClient.get();
            }

            log.createClientFromConfig(config.getTopicName());

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

    private Optional<SnsAsyncClient> getInjectedClient(final SnsClientConfig config) {
        if (!providedClients.isUnsatisfied()) {
            if (providedClients.isAmbiguous()) {
                Instance<SnsAsyncClient> annotatedClient = providedClients
                        .select(Identifier.Literal.of(config.getTopicName()));

                if (annotatedClient.isUnsatisfied()) {
                    annotatedClient = providedClients.select(NamedLiteral.of(config.getTopicName()));

                    if (!annotatedClient.isUnsatisfied()) {
                        ProviderLogging.log.deprecatedNamed();
                    }
                }

                if (annotatedClient.isResolvable()) {
                    return Optional.of(annotatedClient.get());
                }
            } else {
                return Optional.of(providedClients.get());
            }
        }
        return Optional.empty();
    }
}
