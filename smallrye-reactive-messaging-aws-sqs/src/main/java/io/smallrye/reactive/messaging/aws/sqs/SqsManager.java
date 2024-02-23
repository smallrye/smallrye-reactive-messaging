package io.smallrye.reactive.messaging.aws.sqs;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import software.amazon.awssdk.services.sqs.SqsClient;

@ApplicationScoped
public class SqsManager {

    final Map<SqsConfig, SqsClient> clients = new HashMap<>();

    final Map<SqsConfig, String> queueUrls = new HashMap<>();

    public SqsClient getClient(SqsConfig config) {
        return clients.computeIfAbsent(config, c -> {
            var builder = SqsClient
                    .builder();
            if (c.getEndpointOverride().isPresent()) {
                builder.endpointOverride(URI.create(c.getEndpointOverride().get()));
            }
            if (c.getRegion().isPresent()) {
                builder.region(c.getRegion().get());
            }
            builder.credentialsProvider(c.getCredentialsProvider());
            return builder.build();
        });
    }

    public String getQueueUrl(SqsConfig config) {
        return queueUrls.computeIfAbsent(config,
                q -> getClient(q).getQueueUrl(r -> r.queueName(q.getQueueName())).queueUrl());
    }
}
