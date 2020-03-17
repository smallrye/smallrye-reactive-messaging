package io.smallrye.reactive.messaging.gcp.pubsub;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PreDestroy;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Message;

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

@Singleton
public class PubSubManager {

    private static final Map<PubSubConfig, Publisher> PUBLISHER_MAP = new ConcurrentHashMap<>();
    private static final Map<PubSubConfig, TopicAdminClient> TOPIC_ADMIN_CLIENT_MAP = new ConcurrentHashMap<>();
    private static final Map<PubSubConfig, SubscriptionAdminClient> SUBSCRIPTION_ADMIN_CLIENT_MAP = new ConcurrentHashMap<>();

    private static final List<MultiEmitter<? super Message<?>>> EMITTERS = new CopyOnWriteArrayList<>();

    private static final AtomicReference<ManagedChannel> CHANNEL = new AtomicReference<>();

    public Publisher publisher(final PubSubConfig config) {
        return PUBLISHER_MAP.computeIfAbsent(config, PubSubManager::buildPublisher);
    }

    public Subscriber subscriber(PubSubConfig config, MultiEmitter<? super Message<?>> emitter) {
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

        EMITTERS.add(emitter);

        return subscriber;
    }

    public SubscriptionAdminClient subscriptionAdminClient(final PubSubConfig config) {
        return SUBSCRIPTION_ADMIN_CLIENT_MAP.computeIfAbsent(config, PubSubManager::buildSubscriptionAdminClient);
    }

    public TopicAdminClient topicAdminClient(final PubSubConfig config) {
        return TOPIC_ADMIN_CLIENT_MAP.computeIfAbsent(config, PubSubManager::buildTopicAdminClient);
    }

    @PreDestroy
    public void destroy() {
        TOPIC_ADMIN_CLIENT_MAP.values().forEach(topicAdminClient -> {
            try {
                topicAdminClient.shutdown();
                topicAdminClient.awaitTermination(2, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        SUBSCRIPTION_ADMIN_CLIENT_MAP.values().forEach(subscriptionAdminClient -> {
            try {
                subscriptionAdminClient.shutdown();
                subscriptionAdminClient.awaitTermination(2, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        PUBLISHER_MAP.values().forEach(publisher -> {
            try {
                publisher.shutdown();
                publisher.awaitTermination(2, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        EMITTERS.forEach(MultiEmitter::complete);

        if (CHANNEL.get() != null) {
            try {
                CHANNEL.get().shutdown();
                CHANNEL.get().awaitTermination(2, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static SubscriptionAdminClient buildSubscriptionAdminClient(final PubSubConfig config) {
        final SubscriptionAdminSettings.Builder subscriptionAdminSettingsBuilder = SubscriptionAdminSettings.newBuilder();

        buildCredentialsProvider(config).ifPresent(subscriptionAdminSettingsBuilder::setCredentialsProvider);
        buildTransportChannelProvider(config).ifPresent(subscriptionAdminSettingsBuilder::setTransportChannelProvider);

        try {
            return SubscriptionAdminClient.create(subscriptionAdminSettingsBuilder.build());
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to build pub/sub subscription admin client", e);
        }
    }

    private static TopicAdminClient buildTopicAdminClient(final PubSubConfig config) {
        final TopicAdminSettings.Builder topicAdminSettingsBuilder = TopicAdminSettings.newBuilder();

        buildCredentialsProvider(config).ifPresent(topicAdminSettingsBuilder::setCredentialsProvider);
        buildTransportChannelProvider(config).ifPresent(topicAdminSettingsBuilder::setTransportChannelProvider);

        try {
            return TopicAdminClient.create(topicAdminSettingsBuilder.build());
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to build pub/sub topic admin client");
        }
    }

    private static Publisher buildPublisher(final PubSubConfig config) {
        final ProjectTopicName topicName = ProjectTopicName.of(config.getProjectId(), config.getTopic());

        try {
            final Publisher.Builder publisherBuilder = Publisher.newBuilder(topicName);

            buildCredentialsProvider(config).ifPresent(publisherBuilder::setCredentialsProvider);
            buildTransportChannelProvider(config).ifPresent(publisherBuilder::setChannelProvider);

            return publisherBuilder.build();
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to build pub/sub publisher", e);
        }
    }

    private static Subscriber buildSubscriber(final PubSubConfig config, final PubSubMessageReceiver messageReceiver) {
        final ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(config.getProjectId(),
                config.getSubscription());

        final Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(subscriptionName, messageReceiver);

        buildCredentialsProvider(config).ifPresent(subscriberBuilder::setCredentialsProvider);
        buildTransportChannelProvider(config).ifPresent(subscriberBuilder::setChannelProvider);

        return subscriberBuilder.build();
    }

    private static Optional<TransportChannelProvider> buildTransportChannelProvider(final PubSubConfig config) {
        if (config.isMockPubSubTopics()) {
            final ManagedChannel channel = buildChannel(config);

            return Optional.of(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));
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

    private static synchronized ManagedChannel buildChannel(final PubSubConfig config) {
        if (CHANNEL.get() == null) {
            CHANNEL.set(ManagedChannelBuilder.forAddress(config.getHost(), config.getPort())
                    .usePlaintext()
                    .build());
        }
        return CHANNEL.get();
    }
}
