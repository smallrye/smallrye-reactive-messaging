package io.smallrye.reactive.messaging;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderMessages.msg;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.reactive.converters.ReactiveTypeConverter;
import io.smallrye.reactive.converters.Registry;

public class StreamTransformerMediator extends AbstractMediator {

    Function<PublisherBuilder<? extends Message<?>>, PublisherBuilder<? extends Message<?>>> function;

    private PublisherBuilder<? extends Message<?>> publisher;

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
    public void connectToUpstream(PublisherBuilder<? extends Message<?>> publisher) {
        Objects.requireNonNull(function);
        this.publisher = decorate(function.apply(convert(publisher)));
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getStream() {
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
        function = publisher -> {
            PublisherBuilder<Message<?>> prependedWithAck = publisher
                    .flatMapCompletionStage(managePreProcessingAck());

            PublisherBuilder<Message<?>> builder = invoke(prependedWithAck);
            Objects.requireNonNull(builder, msg.methodReturnedNull(configuration.methodAsString()));
            return builder;
        };
    }

    @SuppressWarnings("unchecked")
    private void processMethodConsumingAPublisherOfMessages() {
        function = publisher -> {
            Publisher<Message<?>> prependedWithAck = publisher
                    .flatMapCompletionStage(managePreProcessingAck())
                    .buildRs();
            Class<?> parameterType = configuration.getParameterTypes()[0];
            Optional<? extends ReactiveTypeConverter<?>> converter = Registry.lookup(parameterType);
            if (converter.isPresent()) {
                prependedWithAck = (Publisher<Message<?>>) converter.get().fromPublisher(prependedWithAck);
            }
            Publisher<Message<?>> result = invoke(prependedWithAck);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return ReactiveStreams.fromPublisher(result);
        };
    }

    private void processMethodConsumingAPublisherBuilderOfPayload() {
        function = builder -> {
            PublisherBuilder<Object> unwrapped = builder
                    .flatMapCompletionStage(managePreProcessingAck())
                    .map(Message::getPayload);
            PublisherBuilder<Object> result = invoke(unwrapped);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return result.map(o -> (Message<?>) Message.of(o));
        };
    }

    private void processMethodConsumingAPublisherOfPayload() {
        function = builder -> {
            Publisher<?> stream = builder
                    .flatMapCompletionStage(managePreProcessingAck())
                    .map(Message::getPayload).buildRs();
            // Ability to inject Publisher implementation in method getting a Publisher.
            Class<?> parameterType = configuration.getParameterTypes()[0];
            Optional<? extends ReactiveTypeConverter<?>> converter = Registry.lookup(parameterType);
            if (converter.isPresent()) {
                stream = (Publisher<?>) converter.get().fromPublisher(stream);
            }
            Publisher<Object> result = invoke(stream);
            Objects.requireNonNull(result, msg.methodReturnedNull(configuration.methodAsString()));
            return ReactiveStreams.fromPublisher(result)
                    .map(o -> (Message<?>) Message.of(o));
        };
    }

}
