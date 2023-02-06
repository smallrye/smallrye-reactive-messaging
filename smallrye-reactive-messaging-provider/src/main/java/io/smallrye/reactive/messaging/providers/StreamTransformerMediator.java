package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;
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
import mutiny.zero.flow.adapters.AdaptersToFlow;
import mutiny.zero.flow.adapters.AdaptersToReactiveStreams;

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
        // 1. Flow.Publisher<Message<O>> method(Flow.Publisher<Message<I>> publisher), Publisher<Message<O>> method(Publisher<Message<I>> publisher), PublisherBuilder<Message<O>> method(PublisherBuilder<Message<I>> publisher)
        // 2. Flow.Publisher<O> method(Flow.Publisher<I> publisher), Publisher<O> method(Publisher<I> publisher), PublisherBuilder<O> method(PublisherBuilder<I> publisher)

        switch (configuration.consumption()) {
            case STREAM_OF_MESSAGE:
                // Case 1
                if (configuration.usesBuilderTypes()) {
                    processMethodConsumingAPublisherBuilderOfMessages();
                } else if (configuration.usesReactiveStreams()) {
                    processMethodConsumingAReactiveStreamsPublisherOfMessages();
                } else {
                    processMethodConsumingAPublisherOfMessages();
                }
                break;
            case STREAM_OF_PAYLOAD:
                // Case 2
                if (configuration.usesBuilderTypes()) {
                    processMethodConsumingAPublisherBuilderOfPayload();
                } else if (configuration.usesReactiveStreams()) {
                    processMethodConsumingAReactiveStreamsPublisherOfPayload();
                } else {
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
            PublisherBuilder<? extends Message<?>> argument = ReactiveStreams
                    .fromPublisher(AdaptersToReactiveStreams.publisher(multi));
            PublisherBuilder<Message<?>> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(AdaptersToFlow.publisher(result.buildRs()));
        };
    }

    private void processMethodConsumingAReactiveStreamsPublisherOfMessages() {
        function = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            Publisher<? extends Message<?>> argument = convertToDesiredReactiveStreamPublisherType(multi);
            Publisher<Message<?>> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(AdaptersToFlow.publisher(result));
        };
    }

    private void processMethodConsumingAPublisherOfMessages() {
        function = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            Flow.Publisher<? extends Message<?>> argument = convertToDesiredPublisherType(multi);
            Flow.Publisher<Message<?>> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(result);
        };
    }

    @SuppressWarnings("unchecked")
    private <T> Flow.Publisher<T> convertToDesiredPublisherType(Multi<T> multi) {
        Class<?> parameterType = configuration.getParameterTypes()[0];
        if (parameterType.equals(Multi.class)) {
            return multi;
        }
        Optional<? extends ReactiveTypeConverter<?>> converter = Registry.lookup(parameterType);
        Flow.Publisher<T> argument = multi;
        if (converter.isPresent()) {
            argument = (Flow.Publisher<T>) converter.get().fromFlowPublisher(multi);
        }
        return argument;
    }

    @SuppressWarnings("unchecked")
    private <T> Publisher<T> convertToDesiredReactiveStreamPublisherType(Multi<T> multi) {
        Class<?> parameterType = configuration.getParameterTypes()[0];
        Optional<? extends ReactiveTypeConverter<?>> converter = Registry.lookup(parameterType);
        Publisher<T> argument = AdaptersToReactiveStreams.publisher(multi);
        if (converter.isPresent()) {
            argument = (Publisher<T>) converter.get().fromFlowPublisher(multi);
        }
        return argument;
    }

    private void processMethodConsumingAPublisherBuilderOfPayload() {
        function = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            PublisherBuilder<?> argument = ReactiveStreams.fromPublisher(AdaptersToReactiveStreams.publisher(multi));
            PublisherBuilder<Object> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(AdaptersToFlow.publisher(result.buildRs()))
                    .onItem().transform(Message::of);
        };
    }

    private void processMethodConsumingAReactiveStreamsPublisherOfPayload() {
        function = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            Publisher<?> argument = convertToDesiredReactiveStreamPublisherType(multi);
            Publisher<Object> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return Multi.createFrom().publisher(AdaptersToFlow.publisher(result))
                    .onItem().transform(Message::of);
        };
    }

    private void processMethodConsumingAPublisherOfPayload() {
        function = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            Flow.Publisher<?> argument = convertToDesiredPublisherType(multi);
            Flow.Publisher<Object> result = invoke(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(result)
                    .onItem().transform(Message::of);
        };
    }

}
