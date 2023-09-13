package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.helpers.KeyMultiUtils.convertToKeyedMulti;
import static io.smallrye.reactive.messaging.providers.helpers.KeyMultiUtils.convertToKeyedMultiMessage;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.split.MultiSplitter;
import io.smallrye.reactive.converters.ReactiveTypeConverter;
import io.smallrye.reactive.converters.Registry;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.keyed.KeyedMulti;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import mutiny.zero.flow.adapters.AdaptersToFlow;
import mutiny.zero.flow.adapters.AdaptersToReactiveStreams;

public class StreamTransformerMediator extends AbstractMediator {

    Function<Multi<? extends Message<?>>, Multi<? extends Message<?>>> function;

    private Multi<? extends Message<?>> publisher;

    private final Map<String, Multi<? extends Message<?>>> outgoingPublisherMap = new HashMap<>();

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

        if (configuration.consumption() == MediatorConfiguration.Consumption.KEYED_MULTI
                && configuration.production() == MediatorConfiguration.Production.STREAM_OF_MESSAGE) {
            throw ex.definitionProduceMessageStreamAndConsumePayloadStream(configuration.methodAsString());
        }

        if (configuration.consumption() == MediatorConfiguration.Consumption.KEYED_MULTI_MESSAGE
                && configuration.production() == MediatorConfiguration.Production.STREAM_OF_PAYLOAD) {
            throw ex.definitionProducePayloadStreamAndConsumeMessageStream(configuration.methodAsString());
        }

        if (configuration.consumption() == MediatorConfiguration.Consumption.STREAM_OF_MESSAGE
                && configuration.production() == MediatorConfiguration.Production.SPLIT_MULTI_OF_PAYLOAD) {
            throw ex.definitionProducePayloadStreamAndConsumeMessageStream(configuration.methodAsString());
        }

        if (configuration.consumption() == MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD
                && configuration.production() == MediatorConfiguration.Production.SPLIT_MULTI_OF_MESSAGE) {
            throw ex.definitionProduceMessageStreamAndConsumePayloadStream(configuration.methodAsString());
        }

        if (configuration.consumption() == MediatorConfiguration.Consumption.KEYED_MULTI
                && configuration.production() == MediatorConfiguration.Production.SPLIT_MULTI_OF_MESSAGE) {
            throw ex.definitionProduceMessageStreamAndConsumePayloadStream(configuration.methodAsString());
        }

        if (configuration.consumption() == MediatorConfiguration.Consumption.KEYED_MULTI_MESSAGE
                && configuration.production() == MediatorConfiguration.Production.SPLIT_MULTI_OF_PAYLOAD) {
            throw ex.definitionProducePayloadStreamAndConsumeMessageStream(configuration.methodAsString());
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
    public Multi<? extends Message<?>> getStream(String outgoing) {
        if (configuration.production() == MediatorConfiguration.Production.SPLIT_MULTI_OF_MESSAGE
                || configuration.production() == MediatorConfiguration.Production.SPLIT_MULTI_OF_PAYLOAD) {
            return outgoingPublisherMap.get(outgoing);
        }
        return super.getStream(outgoing);
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
                } else if (configuration.production() == MediatorConfiguration.Production.SPLIT_MULTI_OF_MESSAGE) {
                    processMethodConsumingAPublisherOfPayloadAndProducingSplitMultiOfMessages();
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
                } else if (configuration.production() == MediatorConfiguration.Production.SPLIT_MULTI_OF_PAYLOAD) {
                    processMethodConsumingAPublisherOfPayloadAndProducingSplitMulti();
                } else {
                    processMethodConsumingAPublisherOfPayload();
                }
                break;
            case KEYED_MULTI:
                processMethodConsumingAPublisherOfKeyValue();
                break;
            case KEYED_MULTI_MESSAGE:
                processMethodConsumingAPublisherOfKeyValueMessage();
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

    private void processMethodConsumingAPublisherOfPayloadAndProducingSplitMultiOfMessages() {
        function = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            Flow.Publisher<? extends Message<?>> argument = convertToDesiredPublisherType(multi);
            MultiSplitter<? extends Message<?>, ?> result = fillSplitsOfMessages(argument);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return upstream;
        };
    }

    private <T extends Message<?>, K extends Enum<K>> MultiSplitter<T, K> fillSplitsOfMessages(Flow.Publisher<T> argument) {
        MultiSplitter<T, K> result = invoke(argument);
        Map<K, String> keyChannelMappings = findKeyOutgoingChannelMappings(result.keyType().getEnumConstants());
        keyChannelMappings.forEach((key, outgoing) -> {
            Multi<? extends Message<?>> m = result.get(key)
                    // concat map with prefetch handles the request starvation issue with SplitMulti and also syncs requests
                    .onItem().transformToUni(u -> Uni.createFrom().item(u)).concatenate(true);
            outgoingPublisherMap.put(outgoing, m);
        });
        return result;
    }

    private <K extends Enum<K>> Map<K, String> findKeyOutgoingChannelMappings(K[] enumConstants) {
        List<String> outgoings = configuration.getOutgoings();
        if (outgoings.size() != enumConstants.length) {
            throw ex.outgoingsDoesNotMatchMultiSplitterTarget(getMethodAsString(), outgoings.size(), enumConstants.length);
        }
        Map<K, String> mappings = new HashMap<>();
        for (String outgoing : outgoings) {
            for (K key : enumConstants) {
                if (outgoing.equalsIgnoreCase(key.toString())) {
                    mappings.put(key, outgoing);
                }
            }
        }
        if (mappings.keySet().containsAll(Arrays.asList(enumConstants)) && mappings.values().containsAll(outgoings)) {
            return mappings;
        }
        for (int i = 0; i < outgoings.size(); i++) {
            mappings.put(enumConstants[i], outgoings.get(i));
        }
        return mappings;
    }

    private void processMethodConsumingAPublisherOfMessages() {
        function = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            Flow.Publisher<Message<?>> result = invoke(multi);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return MultiUtils.publisher(result);
        };
    }

    @SuppressWarnings("unchecked")
    private <T> Flow.Publisher<T> convertToDesiredPublisherType(Multi<T> multi) {
        Class<?> parameterType = configuration.getParameterDescriptor().getTypes().get(0);
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
        Class<?> parameterType = configuration.getParameterDescriptor().getTypes().get(0);
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

    private void processMethodConsumingAPublisherOfPayloadAndProducingSplitMulti() {
        function = upstream -> {
            Multi<?> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration)
                    .onItem().transform(Message::getPayload);
            MultiSplitter<?, ?> result = fillSplitFunction(multi);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return upstream;
        };
    }

    private <K extends Enum<K>> MultiSplitter<?, K> fillSplitFunction(Flow.Publisher<?> argument) {
        MultiSplitter<?, K> result = invoke(argument);
        Map<K, String> keyChannelMappings = findKeyOutgoingChannelMappings(result.keyType().getEnumConstants());
        keyChannelMappings.forEach((key, outgoing) -> {
            Multi<? extends Message<?>> m = result.get(key).onItem().transform(Message::of)
                    // concat map with prefetch handles the request starvation issue with SplitMulti and also syncs requests
                    .onItem().transformToUni(u -> Uni.createFrom().item(u)).concatenate(true);
            outgoingPublisherMap.put(outgoing, m);
        });
        return result;
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

    private void processMethodConsumingAPublisherOfKeyValue() {
        function = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            Multi<KeyedMulti<?, ?>> groups = convertToKeyedMulti(multi, extractors(), configuration);
            return groups
                    .flatMap(km -> {
                        Flow.Publisher<?> result = invoke(km);
                        return Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
                    }).map(Message::of);
        };
    }

    private void processMethodConsumingAPublisherOfKeyValueMessage() {
        function = upstream -> {
            Multi<? extends Message<?>> multi = MultiUtils.handlePreProcessingAcknowledgement(upstream, configuration);
            Multi<KeyedMulti<?, Message<?>>> groups = convertToKeyedMultiMessage(multi, extractors(), configuration);
            return groups
                    .flatMap(km -> {
                        Flow.Publisher<Message<?>> result = invoke(km);
                        return Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
                    });
        };
    }

}
