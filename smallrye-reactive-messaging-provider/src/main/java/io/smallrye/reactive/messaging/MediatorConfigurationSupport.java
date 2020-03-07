package io.smallrye.reactive.messaging;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import javax.enterprise.inject.spi.DefinitionException;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.helpers.ClassUtils;

public class MediatorConfigurationSupport {

    private final String methodAsString;
    private final Class<?> returnType;
    private final Class<?>[] parameterTypes;
    private final GenericTypeAssignable returnTypeAssignable;
    private final GenericTypeAssignable firstMethodParamTypeAssignable;
    private final boolean strict;

    public MediatorConfigurationSupport(String methodAsString, Class<?> returnType, Class<?>[] parameterTypes,
            GenericTypeAssignable returnTypeAssignable, GenericTypeAssignable firstMethodParamTypeAssignable,
            boolean strictMode) {
        this.methodAsString = methodAsString;
        this.returnType = returnType;
        this.parameterTypes = parameterTypes;
        this.returnTypeAssignable = returnTypeAssignable;
        this.firstMethodParamTypeAssignable = firstMethodParamTypeAssignable;
        this.strict = strictMode;
    }

    public Shape determineShape(List<?> incomingValue, Object outgoingValue) {
        if (!incomingValue.isEmpty() && outgoingValue != null) {
            if (isPublisherOrPublisherBuilder(returnType)
                    && isConsumingAPublisherOrAPublisherBuilder(parameterTypes)) {
                return Shape.STREAM_TRANSFORMER;
            } else {
                return Shape.PROCESSOR;
            }
        } else if (!incomingValue.isEmpty()) {
            return Shape.SUBSCRIBER;
        } else {
            return Shape.PUBLISHER;
        }
    }

    private boolean isPublisherOrPublisherBuilder(Class<?> returnType) {
        return ClassUtils.isAssignable(returnType, Publisher.class)
                || ClassUtils.isAssignable(returnType, PublisherBuilder.class);
    }

    private boolean isConsumingAPublisherOrAPublisherBuilder(Class[] parameterTypes) {
        if (parameterTypes.length >= 1) {
            Class<?> type = parameterTypes[0];
            return ClassUtils.isAssignable(type, Publisher.class) || ClassUtils.isAssignable(type, PublisherBuilder.class);
        }
        return false;
    }

    public Acknowledgment.Strategy processSuppliedAcknowledgement(List<?> incomings,
            Supplier<Acknowledgment.Strategy> supplier) {
        Acknowledgment.Strategy result = supplier.get();
        if (!incomings.isEmpty()) {
            return result;
        } else if (result != null) {
            throw getOutgoingError("The @Acknowledgment annotation is only supported for method annotated with @Incoming: "
                    + methodAsString);
        }
        return null;
    }

    public ValidationOutput validate(Shape shape, Acknowledgment.Strategy acknowledgment) {
        switch (shape) {
            case SUBSCRIBER:
                return validateSubscriber();
            case PUBLISHER:
                return validatePublisher();
            case PROCESSOR:
                return validateProcessor(acknowledgment);
            case STREAM_TRANSFORMER:
                return validateStreamTransformer(acknowledgment);
            default:
                throw new IllegalStateException("Unknown shape: " + shape);
        }
    }

    private ValidationOutput validateSubscriber() {
        final MediatorConfiguration.Production production = MediatorConfiguration.Production.NONE;

        // Supported signatures:
        // 1. Subscriber<Message<I>> method() or SubscriberBuilder<Message<I>, ?> method()
        // 2. Subscriber<I> method() or SubscriberBuilder<I, ?> method()
        // 3. CompletionStage<Void> method(Message<I> m) - generic parameter must be Void, + Uni variant
        // 4. CompletionStage<Void> method(I i) - generic parameter must be Void, + Uni variant
        // 5. void/? method(Message<I> m) - this signature has been dropped as it forces blocking acknowledgment. Recommendation: use case 3.
        // 6. void method(I i) - return must ve void

        if (ClassUtils.isAssignable(returnType, Subscriber.class)
                || ClassUtils.isAssignable(returnType, SubscriberBuilder.class)) {
            // Case 1 or 2.
            // Validation -> No parameter
            if (parameterTypes.length != 0) {
                throw getIncomingError("when returning a Subscriber or a SubscriberBuilder, no parameters are expected");
            }
            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw getIncomingError("the returned Subscriber must declare a type parameter");
            }
            // Need to distinguish 1 or 2
            MediatorConfiguration.Consumption consumption = assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                    ? MediatorConfiguration.Consumption.STREAM_OF_MESSAGE
                    : MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD;

            return new ValidationOutput(production, consumption);
        }

        if (ClassUtils.isAssignable(returnType, CompletionStage.class)) {
            // Case 3 or 4
            // Expected parameter 1, Message or payload
            if (parameterTypes.length != 1) {
                throw getIncomingError("when returning a CompletionStage, one parameter is expected");
            }
            // This check must be enabled once the TCK is released.
            //            if (strict && returnTypeAssignable.check(Void.class, 0) != GenericTypeAssignable.Result.Assignable) {
            //                throw getIncomingError("when returning a CompletionStage, the generic type must be Void`");
            //            }

            return new ValidationOutput(production,
                    // Distinction between 3 and 4
                    ClassUtils.isAssignable(parameterTypes[0], Message.class) ? MediatorConfiguration.Consumption.MESSAGE
                            : MediatorConfiguration.Consumption.PAYLOAD);
        }

        if (ClassUtils.isAssignable(returnType, Uni.class)) {
            // Case 3 or 4 - Uni variants
            // Expected parameter 1, Message or payload
            if (parameterTypes.length != 1) {
                throw getIncomingError("when returning a Uni, one parameter is expected");
            }
            // This check must be enabled once the TCK is released.
            //            if (strict && returnTypeAssignable.check(Void.class, 0) != GenericTypeAssignable.Result.Assignable) {
            //                throw getIncomingError("when returning a CompletionStage, the generic type must be Void`");
            //            }

            return new ValidationOutput(production,
                    // Distinction between 3 and 4
                    ClassUtils.isAssignable(parameterTypes[0], Message.class) ? MediatorConfiguration.Consumption.MESSAGE
                            : MediatorConfiguration.Consumption.PAYLOAD);
        }

        // Case 5 and 6, void
        if (parameterTypes.length == 1) {
            // TODO Revisit it with injected parameters
            Class<?> param = parameterTypes[0];
            // Distinction between 5 and 6
            MediatorConfiguration.Consumption consumption = ClassUtils.isAssignable(param, Message.class)
                    ? MediatorConfiguration.Consumption.MESSAGE
                    : MediatorConfiguration.Consumption.PAYLOAD;

            // Detect the case 5 that is not supported (anymore, decision taken during the MP reactive hangout Sept. 11th, 2018)
            if (consumption == MediatorConfiguration.Consumption.MESSAGE) {
                throw getIncomingError(
                        "The signature is not supported as it requires 'blocking' acknowledgment, return a CompletionStage<Message<?> instead.");
            }

            // Must be enabled once we update the tCK.
            //            if (strict && !(returnType.equals(Void.class) || returnType.equals(Void.TYPE))) {
            //                throw getIncomingError(
            //                        "The signature is not supported as the produced result would be ignored. The method must return `void`, found "
            //                                + returnType);
            //            }

            return new ValidationOutput(production, consumption);
        }

        throw getIncomingError("Unsupported signature");
    }

    private ValidationOutput validatePublisher() {
        final MediatorConfiguration.Consumption consumption = MediatorConfiguration.Consumption.NONE;

        // Supported signatures:
        // 1. Publisher<Message<O>> method()
        // 2. Publisher<O> method()
        // 3. PublisherBuilder<Message<O>> method()
        // 4. PublisherBuilder<O> method()
        // 5. O method() O cannot be Void
        // 6. Message<O> method()
        // 7. CompletionStage<Message<O>> method(), Uni<Message<O>>
        // 8. CompletionStage<O> method(), Uni<O>

        if (returnType == Void.TYPE) {
            throw getOutgoingError("the method must not be `void`");
        }

        if (parameterTypes.length != 0) {
            throw getOutgoingError("no parameters expected");
        }

        if (ClassUtils.isAssignable(returnType, Publisher.class)) {
            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw getOutgoingError("the returned Publisher must declare a type parameter");
            }

            // Case 1 or 2
            return new ValidationOutput(
                    assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                            ? MediatorConfiguration.Production.STREAM_OF_MESSAGE
                            : MediatorConfiguration.Production.STREAM_OF_PAYLOAD,
                    consumption);
        }

        if (ClassUtils.isAssignable(returnType, PublisherBuilder.class)) {
            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw getOutgoingError("the returned PublisherBuilder must declare a type parameter");
            }

            // Case 3 or 4
            return new ValidationOutput(
                    assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                            ? MediatorConfiguration.Production.STREAM_OF_MESSAGE
                            : MediatorConfiguration.Production.STREAM_OF_PAYLOAD,
                    consumption, true);
        }

        if (ClassUtils.isAssignable(returnType, Message.class)) {
            // Case 6
            return new ValidationOutput(MediatorConfiguration.Production.INDIVIDUAL_MESSAGE, consumption);
        }

        if (ClassUtils.isAssignable(returnType, CompletionStage.class)) {
            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw getOutgoingError("the returned CompletionStage must declare a type parameter");
            }

            // Case 7 and 8
            return new ValidationOutput(
                    assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                            ? MediatorConfiguration.Production.COMPLETION_STAGE_OF_MESSAGE
                            : MediatorConfiguration.Production.COMPLETION_STAGE_OF_PAYLOAD,
                    consumption);
        }

        if (ClassUtils.isAssignable(returnType, Uni.class)) {
            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw getOutgoingError("the returned Uni must declare a type parameter");
            }

            // Case 7 and 8 -> Uni variant
            return new ValidationOutput(
                    assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                            ? MediatorConfiguration.Production.UNI_OF_MESSAGE
                            : MediatorConfiguration.Production.UNI_OF_PAYLOAD,
                    consumption);
        }

        // Case 5
        return new ValidationOutput(MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD, consumption);
    }

    private ValidationOutput validateProcessor(Acknowledgment.Strategy acknowledgment) {
        // Supported signatures:
        // 1.  Processor<Message<I>, Message<O>> method()
        // 2.  Processor<I, O> method()
        // 3.  ProcessorBuilder<Message<I>, Message<O>> method()
        // 4.  ProcessorBuilder<I, O> method()

        // 5.  Publisher<Message<O>> method(Message<I> msg)
        // 6.  Publisher<O> method(I payload)
        // 7.  PublisherBuilder<Message<O>> method(Message<I> msg)
        // 8.  PublisherBuilder<O> method(I payload)

        // 9. Message<O> method(Message<I> msg)
        // 10. O method(I payload)
        // 11. CompletionStage<O> method(I payload) and Uni<O> method(I payload)
        // 12. CompletionStage<Message<O>> method(Message<I> msg) and Uni<Message<O> method(Message<I> msg)

        MediatorConfiguration.Production production;
        MediatorConfiguration.Consumption consumption;
        Boolean useBuilderTypes = null;

        if (ClassUtils.isAssignable(returnType, Processor.class)
                || ClassUtils.isAssignable(returnType, ProcessorBuilder.class)) {
            // Case 1, 2 or 3, 4

            if (parameterTypes.length != 0) {
                throw getIncomingAndOutgoingError("the method must not have parameters");
            }
            GenericTypeAssignable.Result firstGenericParamOfReturn = returnTypeAssignable.check(Message.class, 0);
            if (firstGenericParamOfReturn == GenericTypeAssignable.Result.NotGeneric) {
                throw getIncomingAndOutgoingError("Expected 2 type parameters for the returned Processor");
            }
            consumption = firstGenericParamOfReturn == GenericTypeAssignable.Result.Assignable
                    ? MediatorConfiguration.Consumption.STREAM_OF_MESSAGE
                    : MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD;

            GenericTypeAssignable.Result secondGenericParamOfReturn = returnTypeAssignable.check(Message.class, 1);
            if (secondGenericParamOfReturn == GenericTypeAssignable.Result.NotGeneric) {
                throw getIncomingAndOutgoingError("Expected 2 type parameters for the returned Processor");
            }
            production = secondGenericParamOfReturn == GenericTypeAssignable.Result.Assignable
                    ? MediatorConfiguration.Production.STREAM_OF_MESSAGE
                    : MediatorConfiguration.Production.STREAM_OF_PAYLOAD;

            useBuilderTypes = ClassUtils.isAssignable(returnType, ProcessorBuilder.class);

        } else if (ClassUtils.isAssignable(returnType, Publisher.class)
                || ClassUtils.isAssignable(returnType, PublisherBuilder.class)) {
            // Case 5, 6, 7, 8
            if (parameterTypes.length != 1) {
                throw new IllegalArgumentException("Invalid method annotated with @Outgoing and @Incoming " + methodAsString
                        + " - one parameter expected");
            }

            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw getOutgoingError("Expected a type parameter for the returned Publisher");
            }
            production = assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                    ? MediatorConfiguration.Production.STREAM_OF_MESSAGE
                    : MediatorConfiguration.Production.STREAM_OF_PAYLOAD;

            consumption = ClassUtils.isAssignable(parameterTypes[0], Message.class)
                    ? MediatorConfiguration.Consumption.STREAM_OF_MESSAGE
                    : MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD;

            useBuilderTypes = ClassUtils.isAssignable(returnType, PublisherBuilder.class);
        } else {
            // Case 9, 10, 11, 12
            Class<?> param = parameterTypes[0];

            if (ClassUtils.isAssignable(returnType, CompletionStage.class)) {
                // Case 11 or 12
                GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
                if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                    throw getIncomingAndOutgoingError("Expected a type parameter in the return CompletionStage");
                }

                production = assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                        ? MediatorConfiguration.Production.COMPLETION_STAGE_OF_MESSAGE
                        : MediatorConfiguration.Production.COMPLETION_STAGE_OF_PAYLOAD;
                consumption = ClassUtils.isAssignable(param, Message.class) ? MediatorConfiguration.Consumption.MESSAGE
                        : MediatorConfiguration.Consumption.PAYLOAD;
            } else if (ClassUtils.isAssignable(returnType, Uni.class)) {
                // Case 11 or 12 - Uni variant
                GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
                if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                    throw getIncomingAndOutgoingError("Expected a type parameter in the return Uni");
                }

                production = assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                        ? MediatorConfiguration.Production.UNI_OF_MESSAGE
                        : MediatorConfiguration.Production.UNI_OF_PAYLOAD;
                consumption = ClassUtils.isAssignable(param, Message.class) ? MediatorConfiguration.Consumption.MESSAGE
                        : MediatorConfiguration.Consumption.PAYLOAD;
            } else {
                // Case 9 or 10
                production = ClassUtils.isAssignable(returnType, Message.class)
                        ? MediatorConfiguration.Production.INDIVIDUAL_MESSAGE
                        : MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD;
                consumption = ClassUtils.isAssignable(param, Message.class) ? MediatorConfiguration.Consumption.MESSAGE
                        : MediatorConfiguration.Consumption.PAYLOAD;
            }
        }

        if (production == MediatorConfiguration.Production.INDIVIDUAL_MESSAGE
                && acknowledgment == Acknowledgment.Strategy.POST_PROCESSING) {
            throw new IllegalStateException(
                    "Unsupported acknowledgement policy - POST_PROCESSING not supported when producing messages");
        }

        return new ValidationOutput(production, consumption, useBuilderTypes);
    }

    private ValidationOutput validateStreamTransformer(Acknowledgment.Strategy acknowledgment) {
        // 1.  Publisher<Message<O>> method(Publisher<Message<I>> publisher)
        // 2. Publisher<O> method(Publisher<I> publisher) - Dropped
        // 3. PublisherBuilder<Message<O>> method(PublisherBuilder<Message<I>> publisher)
        // 4. PublisherBuilder<O> method(PublisherBuilder<I> publisher) - Dropped

        // The case 2 and 4 have been dropped because it is not possible to acknowledge the messages automatically as we can't know when
        // the acknowledgment needs to happen. This has been discussed during the MP Reactive hangout, Sept. 11th, 2018.

        // But, they can be managed when ack is set to none or pre-processing(default)

        MediatorConfiguration.Production production;
        MediatorConfiguration.Consumption consumption;
        Boolean useBuilderTypes;

        // The mediator produces and consumes a stream
        GenericTypeAssignable.Result returnTypeGenericCheck = returnTypeAssignable.check(Message.class, 0);
        if (returnTypeGenericCheck == GenericTypeAssignable.Result.NotGeneric) {
            throw getOutgoingError("Expected a type parameter for the returned Publisher");
        }
        production = returnTypeGenericCheck == GenericTypeAssignable.Result.Assignable
                ? MediatorConfiguration.Production.STREAM_OF_MESSAGE
                : MediatorConfiguration.Production.STREAM_OF_PAYLOAD;

        GenericTypeAssignable.Result firstParamTypeGenericCheck = firstMethodParamTypeAssignable.check(Message.class, 0);
        if (firstParamTypeGenericCheck == GenericTypeAssignable.Result.NotGeneric) {
            throw getIncomingError("Expected a type parameter for the consumed Publisher");
        }
        consumption = firstParamTypeGenericCheck == GenericTypeAssignable.Result.Assignable
                ? MediatorConfiguration.Consumption.STREAM_OF_MESSAGE
                : MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD;

        useBuilderTypes = ClassUtils.isAssignable(returnType, PublisherBuilder.class);

        // Post Acknowledgement is not supported
        if (acknowledgment == Acknowledgment.Strategy.POST_PROCESSING) {
            throw getIncomingAndOutgoingError("Automatic post-processing acknowledgment is not supported.");
        }

        // Validate method and be sure we are not in the case 2 and 4.
        if (consumption == MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD
                && (acknowledgment == Acknowledgment.Strategy.MANUAL)) {
            throw getIncomingAndOutgoingError("Consuming a stream of payload is not supported with MANUAL acknowledgment. " +
                    "Use a Publisher<Message<I>> or PublisherBuilder<Message<I>> instead.");
        }

        if (production == MediatorConfiguration.Production.STREAM_OF_PAYLOAD
                && acknowledgment == Acknowledgment.Strategy.MANUAL) {
            throw getIncomingAndOutgoingError("Consuming a stream of payload is not supported with MANUAL acknowledgment. " +
                    "Use a Publisher<Message<I>> or PublisherBuilder<Message<I>> instead.");
        }

        if (useBuilderTypes) {
            //TODO Test validation.

            // Ensure that the parameter is also using the MP Reactive Streams Operator types.
            Class<?> paramClass = parameterTypes[0];
            if (!ClassUtils.isAssignable(paramClass, PublisherBuilder.class)) {
                throw getIncomingAndOutgoingError(
                        "If the method produces a PublisherBuilder, it needs to consume a PublisherBuilder.");
            }
        }

        // TODO Ensure that the parameter is also a publisher builder.

        return new ValidationOutput(production, consumption, useBuilderTypes);
    }

    public Acknowledgment.Strategy processDefaultAcknowledgement(Shape shape, MediatorConfiguration.Consumption consumption) {
        if (shape == Shape.STREAM_TRANSFORMER) {
            return Acknowledgment.Strategy.PRE_PROCESSING;
        } else if (shape == Shape.PROCESSOR && consumption != MediatorConfiguration.Consumption.PAYLOAD) {
            return Acknowledgment.Strategy.PRE_PROCESSING;
        } else if (shape == Shape.SUBSCRIBER
                && (consumption == MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD
                        || consumption == MediatorConfiguration.Consumption.STREAM_OF_MESSAGE)) {
            return Acknowledgment.Strategy.PRE_PROCESSING;
        } else {
            return Acknowledgment.Strategy.POST_PROCESSING;
        }
    }

    public Merge.Mode processMerge(List<?> incomings, Supplier<Merge.Mode> supplier) {
        Merge.Mode result = supplier.get();
        if (incomings != null && !incomings.isEmpty()) {
            return result;
        } else if (result != null) {
            throw getOutgoingError(
                    "The @Merge annotation is only supported for method annotated with @Incoming: " + methodAsString);
        }
        return null;
    }

    public Integer processBroadcast(Object outgoing, Supplier<Integer> supplier) {
        Integer result = supplier.get();
        if (outgoing != null) {
            return result;
        } else if (result != null) {
            throw getIncomingError(
                    "The @Broadcast annotation is only supported for method annotated with @Outgoing: " + methodAsString);
        }
        return null;
    }

    private DefinitionException getOutgoingError(String message) {
        return new DefinitionException("Invalid method annotated with @Outgoing: " + methodAsString + " - " + message);
    }

    private DefinitionException getIncomingError(String message) {
        return new DefinitionException("Invalid method annotated with @Incoming: " + methodAsString + " - " + message);
    }

    private DefinitionException getIncomingAndOutgoingError(String message) {
        return new DefinitionException(
                "Invalid method annotated with @Incoming and @Outgoing: " + methodAsString + " - " + message);
    }

    public static class ValidationOutput {
        private final MediatorConfiguration.Production production;
        private final MediatorConfiguration.Consumption consumption;
        private final Boolean useBuilderTypes;

        public ValidationOutput(MediatorConfiguration.Production production, MediatorConfiguration.Consumption consumption) {
            this(production, consumption, null);
        }

        public ValidationOutput(MediatorConfiguration.Production production, MediatorConfiguration.Consumption consumption,
                Boolean useBuilderTypes) {
            this.production = production;
            this.consumption = consumption;
            this.useBuilderTypes = useBuilderTypes;
        }

        public MediatorConfiguration.Production getProduction() {
            return production;
        }

        public MediatorConfiguration.Consumption getConsumption() {
            return consumption;
        }

        public Boolean getUseBuilderTypes() {
            return useBuilderTypes;
        }
    }

    public interface GenericTypeAssignable {

        Result check(Class<?> target, int index);

        enum Result {
            NotGeneric,
            InvalidIndex,
            NotAssignable,
            Assignable,
        }
    }
}
