package io.smallrye.reactive.messaging.kafka.transactions;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.Typed;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;

@EmitterFactoryFor(KafkaTransactions.class)
@ApplicationScoped
// Wildcard parameterized type is not a legal managed bean
public class KafkaTransactionsFactory implements EmitterFactory<KafkaTransactionsImpl<Object>> {

    @Inject
    KafkaClientService kafkaClientService;

    @Inject
    ChannelRegistry channelRegistry;

    @Override
    public KafkaTransactionsImpl<Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return new KafkaTransactionsImpl<>(configuration, defaultBufferSize, kafkaClientService);
    }

    @Produces
    @Typed(KafkaTransactions.class)
    @Channel("") // Stream name is ignored during type-safe resolution
    <T> KafkaTransactions<T> produceEmitter(InjectionPoint injectionPoint) {
        String channelName = ChannelProducer.getChannelName(injectionPoint);
        return channelRegistry.getEmitter(channelName, KafkaTransactions.class);
    }

}
