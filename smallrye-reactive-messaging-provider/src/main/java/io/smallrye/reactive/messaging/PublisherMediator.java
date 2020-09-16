package io.smallrye.reactive.messaging;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderMessages.msg;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Uni;

public class PublisherMediator extends AbstractMediator {

    private PublisherBuilder<? extends Message<?>> publisher;

    // Supported signatures:
    // 1. Publisher<Message<O>> method()
    // 2. Publisher<O> method()
    // 3. PublisherBuilder<Message<O>> method()
    // 4. PublisherBuilder<O> method()
    // 5. O method() O cannot be Void
    // 6. Message<O> method()
    // 7. CompletionStage<O> method()
    // 8. CompletionStage<Message<O>> method()

    public PublisherMediator(MediatorConfiguration configuration) {
        super(configuration);
        if (configuration.shape() != Shape.PUBLISHER) {
            throw ex.illegalArgumentForPublisherShape(configuration.shape());
        }
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getStream() {
        return Objects.requireNonNull(publisher);
    }

    @Override
    public boolean isConnected() {
        // Easy, not expecting anything
        return true;
    }

    @Override
    protected <T> Uni<T> invokeBlocking(Object... args) {
        return super.<T> invokeBlocking(args)
                .onItem()
                .ifNull()
                .failWith(ex.nullPointerOnInvokeBlocking(this.configuration.methodAsString()));
    }

    @Override
    public void initialize(Object bean) {
        super.initialize(bean);
        switch (configuration.production()) {
            case STREAM_OF_MESSAGE: // 1, 3
                if (configuration.usesBuilderTypes()) {
                    produceAPublisherBuilderOfMessages();
                } else {
                    produceAPublisherOfMessages();
                }
                break;
            case STREAM_OF_PAYLOAD: // 2, 4
                if (configuration.usesBuilderTypes()) {
                    produceAPublisherBuilderOfPayloads();
                } else {
                    produceAPublisherOfPayloads();
                }
                break;
            case INDIVIDUAL_PAYLOAD: // 5
                produceIndividualPayloads();
                break;
            case INDIVIDUAL_MESSAGE: // 6
                produceIndividualMessages();
                break;
            case COMPLETION_STAGE_OF_MESSAGE: // 8
                produceIndividualCompletionStageOfMessages();
                break;
            case COMPLETION_STAGE_OF_PAYLOAD: // 7
                produceIndividualCompletionStageOfPayloads();
                break;
            case UNI_OF_MESSAGE: // 8 - Uni variant
                produceIndividualUniOfMessages();
                break;
            case UNI_OF_PAYLOAD: // 7 - Uni variant
                produceIndividualUniOfPayloads();
                break;
            default:
                throw ex.illegalArgumentForUnexpectedProduction(configuration.production());
        }

        assert this.publisher != null;
    }

    private void produceAPublisherBuilderOfMessages() {
        PublisherBuilder<Message<?>> builder = invoke();
        setPublisher(builder);
    }

    private void setPublisher(PublisherBuilder<Message<?>> publisher) {
        // no conversion for publisher.
        this.publisher = decorate(publisher);
    }

    private <P> void produceAPublisherBuilderOfPayloads() {
        PublisherBuilder<P> builder = invoke();
        setPublisher(builder.map(Message::of));
    }

    private void produceAPublisherOfMessages() {
        setPublisher(ReactiveStreams.fromPublisher(invoke()));
    }

    private <P> void produceAPublisherOfPayloads() {
        Publisher<P> pub = invoke();
        setPublisher(ReactiveStreams.fromPublisher(pub).map(Message::of));
    }

    private <T> void produceIndividualMessages() {
        if (configuration.isBlocking()) {
            setPublisher(ReactiveStreams.<Uni<T>> generate(this::invokeBlocking)
                    .flatMapCompletionStage(Uni::subscribeAsCompletionStage)
                    .map(message -> (Message<?>) message));
        } else {
            setPublisher(ReactiveStreams.generate(() -> {
                Message<?> message = invoke();
                Objects.requireNonNull(message, msg.methodReturnedNull(configuration.methodAsString()));
                return message;
            }));
        }
    }

    private <T> void produceIndividualPayloads() {
        if (configuration.isBlocking()) {
            setPublisher(ReactiveStreams.<Uni<T>> generate(this::invokeBlocking)
                    .flatMapCompletionStage(Uni::subscribeAsCompletionStage)
                    .map(Message::of));
        } else {
            setPublisher(ReactiveStreams.<T> generate(this::invoke)
                    .map(Message::of));
        }
    }

    private void produceIndividualCompletionStageOfMessages() {
        setPublisher(ReactiveStreams.<CompletionStage<Message<?>>> generate(this::invoke)
                .flatMapCompletionStage(Function.identity()));
    }

    private <P> void produceIndividualCompletionStageOfPayloads() {
        setPublisher(ReactiveStreams.<CompletionStage<P>> generate(this::invoke)
                .flatMapCompletionStage(Function.identity())
                .map(Message::of));
    }

    private void produceIndividualUniOfMessages() {
        setPublisher(ReactiveStreams.<CompletionStage<Message<?>>> generate(() -> {
            Uni<Message<?>> uni = this.invoke();
            return uni.subscribeAsCompletionStage();
        })
                .flatMapCompletionStage(Function.identity()));
    }

    private <P> void produceIndividualUniOfPayloads() {
        setPublisher(ReactiveStreams.<CompletionStage<P>> generate(() -> {
            Uni<P> uni = this.invoke();
            return uni.subscribeAsCompletionStage();
        })
                .flatMapCompletionStage(Function.identity())
                .map(Message::of));
    }
}
