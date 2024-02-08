package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsExceptions.ex;
import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging.log;

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
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.utils.SdkAutoCloseable;

@ApplicationScoped
public class SqsManager {

    @Inject
    Instance<SqsAsyncClient> clientInstance;

    private final Map<SqsClientConfig, SqsAsyncClient> clients = new ConcurrentHashMap<>();

    private final Map<SqsClientConfig, String> queueUrls = new ConcurrentHashMap<>();

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        clients.entrySet().stream()
                .filter(e -> e.getKey().isComplete() && e.getValue() != null)
                .map(Map.Entry::getValue)
                .forEach(SdkAutoCloseable::close);
    }

    private SqsAsyncClient getClient(SqsClientConfig config) {
        return clients.computeIfAbsent(config, c -> {
            if (clientInstance.isResolvable() && !c.isComplete()) {
                return clientInstance.get();
            }
            try {
                var builder = SqsAsyncClient.builder();
                if (c.getEndpointOverride() != null) {
                    builder.endpointOverride(URI.create(c.getEndpointOverride()));
                }
                if (c.getRegion() != null) {
                    builder.region(c.getRegion());
                }
                builder.credentialsProvider(config.createCredentialsProvider());
                return builder.build();
            } catch (Exception e) {
                throw new DeploymentException("The required configuration property \"region\" is missing", e);
            }
        });

    }

    public SqsAsyncClient getClient(SqsConnectorCommonConfiguration config) {
        return getClient(new SqsClientConfig(config));
    }

    public Uni<String> getQueueUrl(SqsConnectorCommonConfiguration config) {
        SqsClientConfig clientConfig = new SqsClientConfig(config);
        if (clientConfig.getQueueUrl() != null || queueUrls.containsKey(clientConfig)) {
            return Uni.createFrom().item(queueUrls.computeIfAbsent(clientConfig, c -> {
                log.queueUrlForChannel(config.getChannel(), clientConfig.getQueueUrl());
                return clientConfig.getQueueUrl();
            }));
        } else {
            return Uni.createFrom().completionStage(() -> getClient(clientConfig)
                    .getQueueUrl(r -> r.queueName(clientConfig.getQueueName()).build()))
                    .map(GetQueueUrlResponse::queueUrl)
                    .invoke(queueUrl -> queueUrls.put(clientConfig, queueUrl))
                    .invoke(queueUrl -> log.queueUrlForChannel(config.getChannel(), queueUrl))
                    .onFailure().transform(ex::illegalStateUnableToRetrieveQueueUrl);
        }
    }

}
