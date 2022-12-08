package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.converters.ReactiveTypeConverter;
import io.smallrye.reactive.converters.Registry;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;

public class StreamTransformerMediator extends AbstractMediator {

    Function<Multi<? extends Message<?>>, Multi<? extends Message<?>>> function;

    private Multi<? extends Message<?>> publisher;

    public StreamTransformerMediator(MediatorConfiguration configuration) {
        super(configuration);

        // We can't mix payloads and messages
        if (configuration.consumption() == MediatorConfiguration.Consumption.STREAM_OF_MESSAGE
                && configuration.production() == MediatorConfiguration.Production.STREAM_OF_PAYLOAD) {
            throw ex.definitionProducePayloadStreamAndConsumeMessageStream(configuration.methodAsString());
        }

        if (configuration.consumption() == MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD
                && configuration.production() == MediatorConfiguration.Production.STREAM_OF_MESSAGE) {
            throw ex.definitionProduceMessageStreamAndConsumePayloadStream(configuration.methodAsString());
        }
    }

    @Override
    public void connectToUpstream(Multi<? extends Message<?>> publisher) {
        Objects.requireNonNull(function);
        Multi<? extends Message<?>> converted = convert(publisher);
        this.publisher = decorate(function.apply(converted));
    }

    @Override
    public Multi<? extends Message<?>> getStream() {
        Objects.requireNonNull(publisher);
        return publisher;
    }

    @Override
    public boolean isConnected() {
        return publisher != null;
    }

    @Override
    public void initialize(Object bean) {
        super.initialize(bean);
        // 1. Publisher<Message<O>> method(Publisher<Message<I>> publisher)
        // 2. Publisher<O> method(Publisher<I> publisher)
        // 3. PublisherBuilder<Message<O>> method(PublisherBuilder<Message<I>> publisher)
        // 4. PublisherBuilder<O> method(PublisherBuilder<I> publisher)

        switch (configuration.consumption()) {
            case STREAM_OF_MESSAGE:
                if (configuration.usesBuilderTypes()) {
                    // Case 3
                    processMethodConsumingAPublisherBuilderOfMessages();
                } else {
                    // Case 1
                    processMethodConsumingAPublisherOfMessages();
                }
                break;
            case STREAM_OF_PAYLOAD:
                if (configuration.usesBuilderTypes()) {
                    // Case 4
                    processMethodConsumingAPublisherBuilderOfPayload();
                } else {
                    // Case 2
                    processMethodConsumingAPublisherOfPayload();
                }
                break;
            default:
                throw ex.illegalArgumentForUnexpectedConsumption(configuration.consumption());
        }

        assert function != null;
    }

    private void processMethodConsumingAPublisherBuilderOfMessages() {
        function = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            PublisherBuilder<? extends Message<?>> argument = ReactiveStreams.fromPublisher(multi);
            PublisherBuilder<Message<?>> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(result.buildRs());
        };
    }

    private void processMethodConsumingAPublisherOfMessages() {
        function = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            Publisher<? extends Message<?>> argument = convertToDesiredPublisherType(multi);
            Publisher<Message<?>> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(result);
        };
    }

    @SuppressWarnings("unchecked")
    private <T> Publisher<T> convertToDesiredPublisherType(Multi<T> multi) {
        Class<?> parameterType = configuration.getParameterTypes()[0];
        if (parameterType.equals(Multi.class)) {
            return multi;
        }
        Optional<? extends ReactiveTypeConverter<?>> converter = Registry.lookup(parameterType);
        Publisher<T> argument = multi;
        if (converter.isPresent()) {
            argument = (Publisher<T>) converter.get().fromPublisher(multi);
        }
        return argument;
    }

    private void processMethodConsumingAPublisherBuilderOfPayload() {
        function = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            PublisherBuilder<?> argument = ReactiveStreams.fromPublisher(multi);
            PublisherBuilder<Object> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(result.buildRs())
                    .onItem().transform(Message::of);
        };
    }

    private void processMethodConsumingAPublisherOfPayload() {
        function = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            Publisher<?> argument = convertToDesiredPublisherType(multi);
            Publisher<Object> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(result)
                    .onItem().transform(Message::of);
        };
    }

}
