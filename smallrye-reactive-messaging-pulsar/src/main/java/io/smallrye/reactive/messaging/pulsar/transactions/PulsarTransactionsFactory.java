package io.smallrye.reactive.messaging.pulsar.transactions;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.Typed;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.pulsar.PulsarClientService;
import io.smallrye.reactive.messaging.pulsar.PulsarConnector;

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

    @Override
    public MessagePublisherProvider<?> createEmitter(EmitterConfiguration configuration, long defaultBufferSize,
            Config channelConfig) {
        if (EmitterFactory.isConnector(channelConfig, PulsarConnector.CONNECTOR_NAME)) {
            return createEmitter(configuration, defaultBufferSize);
        }
        return new NoOpPulsarTransactionsImpl<>(configuration, defaultBufferSize);
    }

    @Produces
    @Typed(PulsarTransactions.class)
    @Channel("") // Stream name is ignored during type-safe resolution
    <T> PulsarTransactions<T> produceEmitter(InjectionPoint injectionPoint) {
        String channelName = ChannelProducer.getChannelName(injectionPoint);
        return channelRegistry.getEmitter(channelName, PulsarTransactions.class);
    }
}
