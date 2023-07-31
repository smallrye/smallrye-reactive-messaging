package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.helpers.CDIUtils.getSortedInstances;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.*;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.HealthCenter;
import io.smallrye.reactive.messaging.providers.helpers.BroadcastHelper;
import io.smallrye.reactive.messaging.providers.helpers.ConverterUtils;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public abstract class AbstractMediator {

    protected final MediatorConfiguration configuration;
    private final Function<Message<?>, Object[]> methodArgumentMapper;
    protected WorkerPoolRegistry workerPoolRegistry;
    private Invoker invoker;
    private Instance<PublisherDecorator> decorators;

    private Instance<SubscriberDecorator> subscriberDecorators;
    protected HealthCenter health;
    private Instance<MessageConverter> converters;
    private Instance<KeyValueExtractor> extractors;
    private int maxConcurrency;

    public AbstractMediator(MediatorConfiguration configuration) {
        this.configuration = configuration;

        Function<Message<?>, Object[]> mapper = null;
        if (configuration.consumption() == MediatorConfiguration.Consumption.MESSAGE) {
            mapper = msg -> new Object[] { msg };
        } else if (configuration.consumption() == MediatorConfiguration.Consumption.PAYLOAD) {
            if (configuration.getParameterDescriptor().getTypes().size() == 1) {
                mapper = msg -> new Object[] { msg.getPayload() };
            } else {
                List<Class<?>> parameters = configuration.getParameterDescriptor().getTypes();
                @SuppressWarnings("unchecked")
                Function<Message<?>, Object>[] extractors = new Function[parameters.size()];
                for (int i = 0; i < parameters.size(); i++) {
                    if (parameters.get(i) == configuration.getIngestedPayloadType()) {
                        extractors[i] = Message::getPayload;
                    } else if (parameters.get(i) == Optional.class) {
                        Class<?> c = configuration.getParameterDescriptor().getGenericParameterType(i, 0);
                        extractors[i] = msg -> msg.getMetadata().get(c);
                    } else {
                        Class<?> type = parameters.get(i);
                        extractors[i] = msg -> msg.getMetadata().get(type).orElse(null);
                    }
                }
                mapper = msg -> Arrays.stream(extractors).map(extractor -> extractor.apply(msg)).toArray(Object[]::new);
            }
        }
        this.methodArgumentMapper = mapper;
    }

    public synchronized void setInvoker(Invoker invoker) {
        this.invoker = invoker;
    }

    public void setDecorators(Instance<PublisherDecorator> decorators) {
        this.decorators = decorators;
    }

    public void setSubscriberDecorators(Instance<SubscriberDecorator> decorators) {
        this.subscriberDecorators = decorators;
    }

    public void setConverters(Instance<MessageConverter> converters) {
        this.converters = converters;
    }

    public void setExtractors(Instance<KeyValueExtractor> extractors) {
        this.extractors = extractors;
    }

    public void setWorkerPoolRegistry(WorkerPoolRegistry workerPoolRegistry) {
        this.workerPoolRegistry = workerPoolRegistry;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public void run() {
        // Do nothing by default.
    }

    public void connectToUpstream(Multi<? extends Message<?>> publisher) {
        // Do nothing by default.
    }

    public MediatorConfiguration configuration() {
        return configuration;
    }

    public void initialize(Object bean) {
        // Method overriding initialize MUST call super(bean).
        synchronized (this) {
            if (this.invoker == null) {
                this.invoker = args -> {
                    try {
                        return this.configuration.getMethod().invoke(bean, args);
                    } catch (Exception e) {
                        throw ex.processingException(configuration.methodAsString(), e);
                    }
                };
            }
        }
        Objects.requireNonNull(this.invoker, msg.invokerNotInitialized());
        if (this.configuration.isBlocking()) {
            Objects.requireNonNull(this.workerPoolRegistry, msg.workerPoolNotInitialized());
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T invoke(Object... args) {
        try {
            return (T) this.invoker.invoke(args);
        } catch (RuntimeException e) { // NOSONAR
            log.methodException(configuration().methodAsString(), e);
            throw e;
        }
    }

    protected <T> Uni<T> invokeOnMessageContext(Message<?> message, Object... args) {
        return LocalContextMetadata.invokeOnMessageContext(message, x -> invoke(args));
    }

    protected <T> Object[] getArguments(Message<T> message) {
        if (methodArgumentMapper != null) {
            return methodArgumentMapper.apply(message);
        } else {
            throw new IllegalArgumentException("Unable to use the argument mapper for method " + configuration.methodAsString()
                    + ", only methods consuming messages or payloads are supported");
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> Uni<T> invokeBlocking(Message<?> message, Object... args) {
        try {
            Optional<LocalContextMetadata> metadata = message != null ? message.getMetadata().get(LocalContextMetadata.class)
                    : Optional.empty();
            Context currentContext = metadata.map(m -> Context.newInstance(m.context()))
                    .orElseGet(Vertx::currentContext);
            return workerPoolRegistry.executeWork(currentContext,
                    Uni.createFrom().deferred(() -> {
                        try {
                            Object result = this.invoker.invoke(args);
                            if (result instanceof CompletionStage) {
                                return Uni.createFrom().completionStage((CompletionStage<T>) result);
                            } else if (result instanceof Uni) {
                                return (Uni<T>) result;
                            } else {
                                return Uni.createFrom().item((T) result);
                            }
                        } catch (RuntimeException e) {
                            log.methodException(configuration().methodAsString(), e);
                            return Uni.createFrom().failure(e);
                        }
                    }),
                    configuration.getWorkerPoolName(),
                    configuration.isBlockingExecutionOrdered());
        } catch (RuntimeException e) {
            log.methodException(configuration().methodAsString(), e);
            throw e;
        }
    }

    protected CompletionStage<Message<?>> getAckOrCompletion(Message<?> message) {
        CompletionStage<Void> ack = message.ack();
        if (ack != null) {
            return ack.thenApply(x -> message);
        } else {
            return CompletableFuture.completedFuture(message);
        }
    }

    public Multi<? extends Message<?>> getStream() {
        return null;
    }

    public Multi<? extends Message<?>> getStream(String outgoing) {
        if (configuration.hasTargetedOutput()) {
            return getStream().onItem().transformToUniAndConcatenate(message -> extractTargetedMessage(outgoing, message));
        }
        return getStream();
    }

    public MediatorConfiguration getConfiguration() {
        return configuration;
    }

    public String getMethodAsString() {
        return configuration.methodAsString();
    }

    public Flow.Subscriber<Message<?>> getComputedSubscriber() {
        return null;
    }

    public abstract boolean isConnected();

    protected Function<Message<?>, ? extends CompletionStage<? extends Message<?>>> managePreProcessingAck() {
        return this::handlePreProcessingAck;
    }

    protected CompletionStage<Message<?>> handlePreProcessingAck(Message<?> message) {
        if (configuration.getAcknowledgment() == Acknowledgment.Strategy.PRE_PROCESSING) {
            return getAckOrCompletion(message);
        }
        return CompletableFuture.completedFuture(message);
    }

    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> input) {
        if (input == null) {
            return null;
        }

        for (PublisherDecorator decorator : getSortedInstances(decorators)) {
            input = decorator.decorate(input, getConfiguration().getOutgoings(), false);
        }

        if (getBroadcast()) {
            if (configuration.hasTargetedOutput() && !configuration.production().isMessageType()) {
                input = input.map(s -> {
                    TargetedMessages messages = TargetedMessages.from((Targeted) s.getPayload());
                    return Messages.chain(s).with(messages.getPayload());
                });
            }
            return BroadcastHelper.broadcastPublisher(input, getNumberOfSubscriberBeforeConnecting());
        } else {
            return input;
        }
    }

    boolean getBroadcast() {
        return configuration.getBroadcast() || configuration.getOutgoings().size() > 1;
    }

    int getNumberOfSubscriberBeforeConnecting() {
        int outgoings = configuration.getOutgoings().size();
        if (outgoings > 1) {
            return outgoings;
        } else {
            return configuration.getNumberOfSubscriberBeforeConnecting();
        }
    }

    public Multi<? extends Message<?>> decorateSubscriberSource(Multi<? extends Message<?>> input) {
        if (input == null) {
            return null;
        }

        for (SubscriberDecorator decorator : getSortedInstances(subscriberDecorators)) {
            input = decorator.decorate(input, configuration.getIncoming(), false);
        }
        return input;
    }

    public void setHealth(HealthCenter health) {
        this.health = health;
    }

    public Multi<? extends Message<?>> convert(Multi<? extends Message<?>> upstream) {
        return ConverterUtils.convert(upstream, converters, configuration.getIngestedPayloadType());
    }

    public Instance<KeyValueExtractor> extractors() {
        return extractors;
    }

    public int maxConcurrency() {
        return maxConcurrency;
    }

    public void terminate() {
        // Do nothing by default.
    }

    protected Uni<? extends Message<?>> extractTargetedMessage(String outgoing, Message<?> message) {
        if (message instanceof TargetedMessages) {
            Message<?> msg = ((TargetedMessages) message).get(outgoing);
            return Uni.createFrom().item(msg);
        }
        return Uni.createFrom().item(message);
    }

}
