package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import mutiny.zero.flow.adapters.AdaptersToFlow;

public class PublisherMediator extends AbstractMediator {

    private Multi<? extends Message<?>> publisher;

    // Supported signatures:
    // 1. Flow.Publisher<Message<O>> method(), Publisher<Message<O>>, PublisherBuilder<Message<O>>
    // 2. Flow.Publisher<O> method(), Publisher<O>, PublisherBuilder<O>
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
    public Multi<? extends Message<?>> getStream() {
        return Objects.requireNonNull(publisher);
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    protected <T> Uni<T> invokeBlocking(Object... args) {
        return super.<T> invokeBlocking(null, args)
                .onItem().invoke(item -> {
                    // The item must not be null.
                    if (item == null) {
                        throw ex.nullPointerOnInvokeBlocking(this.configuration.methodAsString());
                    }
                });
    }

    @Override
    public void initialize(Object bean) {
        super.initialize(bean);
        switch (configuration.production()) {
            case STREAM_OF_MESSAGE: // 1
                if (configuration.usesBuilderTypes()) {
                    produceAPublisherBuilderOfMessages();
                } else if (configuration.usesReactiveStreams()) {
                    produceAReactiveStreamsPublisherOfMessages();
                } else {
                    produceAPublisherOfMessages();
                }
                break;
            case STREAM_OF_PAYLOAD: // 2
                if (configuration.usesBuilderTypes()) {
                    produceAPublisherBuilderOfPayloads();
                } else if (configuration.usesReactiveStreams()) {
                    produceAReactiveStreamsPublisherOfPayloads();
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
        this.publisher = decorate(MultiUtils.publisher(AdaptersToFlow.publisher(builder.buildRs())));
    }

    private <P> void produceAPublisherBuilderOfPayloads() {
        PublisherBuilder<P> builder = invoke();
        this.publisher = decorate(MultiUtils.publisher(AdaptersToFlow.publisher(builder.map(Message::of).buildRs())));
    }

    private void produceAPublisherOfMessages() {
        Flow.Publisher<Message<?>> pub = invoke();
        this.publisher = MultiUtils.publisher(pub);
    }

    private void produceAReactiveStreamsPublisherOfMessages() {
        Publisher<Message<?>> pub = invoke();
        this.publisher = MultiUtils.publisher(AdaptersToFlow.publisher(pub));
    }

    private <P> void produceAPublisherOfPayloads() {
        Flow.Publisher<P> pub = invoke();
        this.publisher = decorate(MultiUtils.publisher(pub).map(Message::of));
    }

    private <P> void produceAReactiveStreamsPublisherOfPayloads() {
        Publisher<P> pub = invoke();
        this.publisher = decorate(Multi.createFrom().publisher(AdaptersToFlow.publisher(pub)).map(Message::of));
    }

    private void produceIndividualMessages() {
        if (configuration.isBlocking()) {
            if (configuration.isBlockingExecutionOrdered()) {
                this.publisher = decorate(MultiUtils.createFromGenerator(this::invokeBlocking)
                        .onItem().transformToUniAndConcatenate(u -> u)
                        .onItem().transform(o -> (Message<?>) o));
            } else {
                this.publisher = decorate(MultiUtils.createFromGenerator(this::invokeBlocking)
                        .onItem().transformToUniAndMerge(u -> u)
                        .onItem().transform(o -> (Message<?>) o));
            }
        } else {
            this.publisher = decorate(MultiUtils.createFromGenerator(() -> {
                Message<?> message = invoke();
                Objects.requireNonNull(message, msg.methodReturnedNull(configuration.methodAsString()));
                return message;
            }));
        }
    }

    private void produceIndividualPayloads() {
        if (configuration.isBlocking()) {
            if (configuration.isBlockingExecutionOrdered()) {
                this.publisher = decorate(MultiUtils.createFromGenerator(this::invokeBlocking)
                        .onItem().transformToUniAndConcatenate(u -> u)
                        .onItem().transform(Message::of));
            } else {
                this.publisher = decorate(MultiUtils.createFromGenerator(this::invokeBlocking)
                        .onItem().transformToUniAndMerge(u -> u)
                        .onItem().transform(Message::of));
            }
        } else {
            this.publisher = decorate(MultiUtils.createFromGenerator(this::invoke)
                    .onItem().transform(Message::of));
        }
    }

    private void produceIndividualCompletionStageOfMessages() {
        this.publisher = decorate(MultiUtils.<CompletionStage<Message<?>>> createFromGenerator(this::invoke)
                .onItem().transformToUniAndConcatenate(cs -> Uni.createFrom().completionStage(cs)));
    }

    private <P> void produceIndividualCompletionStageOfPayloads() {
        this.publisher = decorate(MultiUtils.<CompletionStage<P>> createFromGenerator(this::invoke)
                .onItem().transformToUniAndConcatenate(cs -> Uni.createFrom().completionStage(cs).map(Message::of)));
    }

    private void produceIndividualUniOfMessages() {
        this.publisher = decorate(MultiUtils.<Uni<Message<?>>> createFromGenerator(this::invoke)
                .onItem().transformToUniAndConcatenate(Function.identity()));
    }

    private void produceIndividualUniOfPayloads() {
        this.publisher = decorate(MultiUtils.<Uni<?>> createFromGenerator(this::invoke)
                .onItem().transformToUniAndConcatenate(u -> u.map(Message::of)));
    }
}
