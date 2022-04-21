package io.smallrye.reactive.messaging.gcp.pubsub;

import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubExceptions.ex;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.smallrye.mutiny.subscription.MultiEmitter;

@ApplicationScoped
public class PubSubManager {

    private final Map<PubSubConfig, Publisher> publishers = new ConcurrentHashMap<>();
    private final Map<PubSubConfig, TopicAdminClient> topicAdminClients = new ConcurrentHashMap<>();
    private final Map<PubSubConfig, SubscriptionAdminClient> subscriptionAdminClients = new ConcurrentHashMap<>();

    private final List<MultiEmitter<? super Message<?>>> emitters = new CopyOnWriteArrayList<>();
    private final List<ManagedChannel> channels = new CopyOnWriteArrayList<>();

    public Publisher publisher(final PubSubConfig config) {
        return publishers.computeIfAbsent(config, this::buildPublisher);
    }

    public void subscriber(PubSubConfig config, MultiEmitter<? super Message<?>> emitter) {
        final Subscriber subscriber = buildSubscriber(config, new PubSubMessageReceiver(emitter));
        emitter.onTermination(() -> {
            subscriber.stopAsync();
            try {
                subscriber.awaitTerminated(2, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                // Ignore it.
            }
        });
        subscriber.startAsync();

        emitters.add(emitter);
    }

    public SubscriptionAdminClient subscriptionAdminClient(final PubSubConfig config) {
        return subscriptionAdminClients.computeIfAbsent(config, this::buildSubscriptionAdminClient);
    }

    public TopicAdminClient topicAdminClient(final PubSubConfig config) {
        return topicAdminClients.computeIfAbsent(config, this::buildTopicAdminClient);
    }

    @PreDestroy
    public void destroy() {
        topicAdminClients.values().forEach(PubSubManager::shutdown);
        topicAdminClients.clear();

        subscriptionAdminClients.values().forEach(PubSubManager::shutdown);
        subscriptionAdminClients.clear();

        publishers.values().forEach(publisher -> {
            try {
                publisher.shutdown();
                publisher.awaitTermination(2, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        publishers.clear();

        emitters.forEach(MultiEmitter::complete);
        emitters.clear();

        channels.forEach(channel -> {
            try {
                channel.shutdown();
                channel.awaitTermination(2, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        channels.clear();
    }

    private SubscriptionAdminClient buildSubscriptionAdminClient(final PubSubConfig config) {
        final SubscriptionAdminSettings.Builder subscriptionAdminSettingsBuilder = SubscriptionAdminSettings.newBuilder();

        buildCredentialsProvider(config).ifPresent(subscriptionAdminSettingsBuilder::setCredentialsProvider);
        buildTransportChannelProvider(config).ifPresent(subscriptionAdminSettingsBuilder::setTransportChannelProvider);

        try {
            return SubscriptionAdminClient.create(subscriptionAdminSettingsBuilder.build());
        } catch (final IOException e) {
            throw ex.illegalStateUnableToBuildSubscriptionAdminClient(e);
        }
    }

    private TopicAdminClient buildTopicAdminClient(final PubSubConfig config) {
        final TopicAdminSettings.Builder topicAdminSettingsBuilder = TopicAdminSettings.newBuilder();

        buildCredentialsProvider(config).ifPresent(topicAdminSettingsBuilder::setCredentialsProvider);
        buildTransportChannelProvider(config).ifPresent(topicAdminSettingsBuilder::setTransportChannelProvider);

        try {
            return TopicAdminClient.create(topicAdminSettingsBuilder.build());
        } catch (final IOException e) {
            throw ex.illegalStateUnableToBuildTopicAdminClient(e);
        }
    }

    private Publisher buildPublisher(final PubSubConfig config) {
        final ProjectTopicName topicName = ProjectTopicName.of(config.getProjectId(), config.getTopic());

        try {
            final Publisher.Builder publisherBuilder = Publisher.newBuilder(topicName);

            buildCredentialsProvider(config).ifPresent(publisherBuilder::setCredentialsProvider);
            buildTransportChannelProvider(config).ifPresent(publisherBuilder::setChannelProvider);

            return publisherBuilder.build();
        } catch (final IOException e) {
            throw ex.illegalStateUnableToBuildPublisher(e);
        }
    }

    private Subscriber buildSubscriber(final PubSubConfig config, final PubSubMessageReceiver messageReceiver) {
        final ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(config.getProjectId(),
                config.getSubscription());

        final Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(subscriptionName, messageReceiver);

        buildCredentialsProvider(config).ifPresent(subscriberBuilder::setCredentialsProvider);
        buildTransportChannelProvider(config).ifPresent(subscriberBuilder::setChannelProvider);

        return subscriberBuilder.build();
    }

    private Optional<TransportChannelProvider> buildTransportChannelProvider(final PubSubConfig config) {
        if (config.isMockPubSubTopics()) {
            return Optional.of(FixedTransportChannelProvider.create(GrpcTransportChannel.create(buildChannel(config))));
        }

        return Optional.empty();
    }

    private static Optional<CredentialsProvider> buildCredentialsProvider(final PubSubConfig config) {
        if (config.isMockPubSubTopics()) {
            return Optional.of(NoCredentialsProvider.create());
        }

        if (config.getCredentialPath() != null) {
            try {
                return Optional.of(FixedCredentialsProvider
                        .create(ServiceAccountCredentials.fromStream(Files.newInputStream(config.getCredentialPath()))));
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }

        return Optional.empty();
    }

    private ManagedChannel buildChannel(final PubSubConfig config) {
        final ManagedChannel channel = ManagedChannelBuilder.forAddress(config.getHost(), config.getPort())
                .usePlaintext()
                .build();
        channels.add(channel);
        return channel;
    }

    private static void shutdown(final BackgroundResource resource) {
        try {
            resource.shutdown();
            resource.awaitTermination(2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
