package io.smallrye.reactive.messaging;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MediatorFactory {

    public AbstractMediator create(MediatorConfiguration configuration) {
        switch (configuration.shape()) {
            case PROCESSOR:
                return new ProcessorMediator(configuration);
            case SUBSCRIBER:
                return new SubscriberMediator(configuration);
            case PUBLISHER:
                return new PublisherMediator(configuration);
            case STREAM_TRANSFORMER:
                return new StreamTransformerMediator(configuration);
            default:
                throw ex.illegalArgumentForUnsupportedShape(configuration.shape(),
                        configuration.methodAsString());
        }
    }

}
