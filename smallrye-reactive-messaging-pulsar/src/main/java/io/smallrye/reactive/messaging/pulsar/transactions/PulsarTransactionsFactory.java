package io.smallrye.reactive.messaging.pulsar.transactions;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.Typed;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.pulsar.PulsarClientService;

@EmitterFactoryFor(PulsarTransactions.class)
@ApplicationScoped
public class PulsarTransactionsFactory implements EmitterFactory<PulsarTransactionsImpl<Object>> {

    @Inject
    PulsarClientService pulsarClientService;

    @Inject
    ChannelRegistry channelRegistry;

    @Override
    public PulsarTransactionsImpl<Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return new PulsarTransactionsImpl<>(configuration, defaultBufferSize, pulsarClientService);
    }

    @Produces
    @Typed(PulsarTransactions.class)
    @Channel("") // Stream name is ignored during type-safe resolution
    <T> PulsarTransactions<T> produceEmitter(InjectionPoint injectionPoint) {
        String channelName = ChannelProducer.getChannelName(injectionPoint);
        return channelRegistry.getEmitter(channelName, PulsarTransactions.class);
    }
}
