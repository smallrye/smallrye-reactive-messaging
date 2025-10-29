package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.providers.impl.ConcurrencyConnectorConfig.*;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.GroupedMulti;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.kafka.ProcessingOrder;
import io.smallrye.reactive.messaging.kafka.TopicPartitionKey;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.providers.helpers.PausableMulti;
import io.smallrye.reactive.messaging.providers.impl.ConnectorConfig;

@ApplicationScoped
public class KeyBasedOrderingDecorator implements PublisherDecorator {

    private final Config config;

    @Inject
    public KeyBasedOrderingDecorator(Instance<Config> rootConfig) {
        this.config = rootConfig.get();
    }

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
            boolean isConnector) {
        if (isConnector) {
            String channel = channelName.get(0);
            if (isConcurrencyChannelName(channel)) {
                return publisher;
            }
            ConnectorConfig connectorConfig = new ConnectorConfig("mp.messaging.incoming.", config, channel);
            Optional<String> orderCfg = connectorConfig.getOptionalValue("throttled.processing-order", String.class);
            ProcessingOrder processingOrder = ProcessingOrder.of(orderCfg.orElse(null));
            Multi<? extends GroupedMulti<TopicPartitionKey, ? extends Message<?>>> groupBy;
            switch (processingOrder) {
                case ORDERED_BY_KEY -> groupBy = publisher.group()
                        .by(message -> TopicPartitionKey.ofKey(getMetadata(message).getRecord()));
                case ORDERED_BY_PARTITION -> groupBy = publisher.group()
                        .by(message -> TopicPartitionKey.ofPartition(getMetadata(message).getRecord()));
                default -> {
                    return publisher;
                }
            }

            return groupBy
                    .onItem().transformToMulti(g -> {
                        PausableMulti<? extends Message<?>> pausable = new PausableMulti<>(g, false);
                        return pausable
                                .invoke(pausable::pause)
                                .map(m -> m.withAck(() -> m.ack().thenRun(pausable::resume)));
                    })
                    .merge(128);
        }
        return publisher;
    }

    private static IncomingKafkaRecordMetadata getMetadata(Message<?> message) {
        return message.getMetadata(IncomingKafkaRecordMetadata.class).get();
    }

}
