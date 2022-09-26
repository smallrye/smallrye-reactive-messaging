package io.smallrye.reactive.messaging.providers;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.providers.helpers.ClassUtils;

public class MediatorConfigurationSupport {

    private final String methodAsString;
    private final Class<?> returnType;
    private final Class<?>[] parameterTypes;
    private final GenericTypeAssignable returnTypeAssignable;
    private final GenericTypeAssignable firstMethodParamTypeAssignable;
    private boolean strict;

    public MediatorConfigurationSupport(String methodAsString, Class<?> returnType, Class<?>[] parameterTypes,
            GenericTypeAssignable returnTypeAssignable, GenericTypeAssignable firstMethodParamTypeAssignable) {
        this.methodAsString = methodAsString;
        this.returnType = returnType;
        this.parameterTypes = parameterTypes;
        this.returnTypeAssignable = returnTypeAssignable;
        this.firstMethodParamTypeAssignable = firstMethodParamTypeAssignable;
    }

    public Shape determineShape(List<?> incomingValue, Object outgoingValue) {
        if (!incomingValue.isEmpty() && outgoingValue != null) {
            if (isPublisherOrReactiveStreamsPublisherOrPublisherBuilder(returnType)
                    && isConsumingAPublisherOrReactiveStreamsPublisherOrAPublisherBuilder(parameterTypes)) {
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

    private boolean isPublisherOrReactiveStreamsPublisherOrPublisherBuilder(Class<?> returnType) {
        return ClassUtils.isAssignable(returnType, Flow.Publisher.class)
                || ClassUtils.isAssignable(returnType, Publisher.class)
                || ClassUtils.isAssignable(returnType, PublisherBuilder.class);
    }

    private boolean isConsumingAPublisherOrReactiveStreamsPublisherOrAPublisherBuilder(Class<?>[] parameterTypes) {
        if (parameterTypes.length >= 1) {
            Class<?> type = parameterTypes[0];
            return ClassUtils.isAssignable(type, Flow.Publisher.class)
                    || ClassUtils.isAssignable(type, Publisher.class)
                    || ClassUtils.isAssignable(type, PublisherBuilder.class);
        }
        return false;
    }

    public Acknowledgment.Strategy processSuppliedAcknowledgement(List<?> incomings,
            Supplier<Acknowledgment.Strategy> supplier) {
        Acknowledgment.Strategy result = supplier.get();
        if (!incomings.isEmpty()) {
            return result;
        } else if (result != null) {
            throw ex.definitionExceptionUnsupported("@Outgoing", methodAsString);
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
                throw ex.illegalStateExceptionForValidate(shape);
        }
    }

    private ValidationOutput validateSubscriber() {
        final MediatorConfiguration.Production production = MediatorConfiguration.Production.NONE;

        // Supported signatures:
        // 1. Flow.Subscriber<Message<I>> method() or Subscriber<Message<I>> method() or SubscriberBuilder<Message<I>, ?> method()
        // 2. Flow.Subscriber<I> method() or Subscriber<I> method() or SubscriberBuilder<I, ?> method()
        // 3. CompletionStage<Void> method(Message<I> m) - generic parameter must be Void, + Uni variant
        // 4. CompletionStage<Void> method(I i) - generic parameter must be Void, + Uni variant
        // 5. void/? method(Message<I> m) - this signature has been dropped as it forces blocking acknowledgment. Recommendation: use case 3.
        // 6. void method(I i) - return must ve void

        if (ClassUtils.isAssignable(returnType, Flow.Subscriber.class)
                || ClassUtils.isAssignable(returnType, Subscriber.class)
                || ClassUtils.isAssignable(returnType, SubscriberBuilder.class)) {
            // Case 1 or 2.
            // Validation -> No parameter
            if (parameterTypes.length != 0) {
                throw ex.definitionNoParamOnSubscriber("@Incoming", methodAsString);
            }
            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw ex.definitionSubscriberTypeParam("@Incoming", methodAsString);
            }
            // Need to distinguish 1 or 2
            MediatorConfiguration.Consumption consumption;
            Type payloadType;
            if (assignableToMessageCheck == GenericTypeAssignable.Result.Assignable) {
                consumption = MediatorConfiguration.Consumption.STREAM_OF_MESSAGE;
                payloadType = returnTypeAssignable.getType(0, 0);
            } else {
                consumption = MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD;
                payloadType = returnTypeAssignable.getType(0);
            }

            boolean useBuilderType = ClassUtils.isAssignable(returnType, SubscriberBuilder.class);
            boolean useReactiveStreams = ClassUtils.isAssignable(returnType, Subscriber.class);
            if (payloadType == null) {
                log.unableToExtractIngestedPayloadType(methodAsString,
                        "Cannot extract the type from the method signature");
            }
            return new ValidationOutput(production, consumption, useBuilderType, useReactiveStreams, payloadType);
        }

        if (ClassUtils.isAssignable(returnType, CompletionStage.class)) {
            // Case 3 or 4
            // Expected parameter 1, Message or payload
            if (parameterTypes.length != 1) {
                throw ex.definitionOnParam("@Incoming", methodAsString, "CompletionStage");
            }
            if (strict && returnTypeAssignable.check(Void.class, 0) != GenericTypeAssignable.Result.Assignable) {
                throw ex.definitionCompletionStageOfVoid(methodAsString);
            }

            MediatorConfiguration.Consumption consumption;
            Type payloadType;
            // Distinction between 3 and 4
            if (ClassUtils.isAssignable(parameterTypes[0], Message.class)) {
                consumption = MediatorConfiguration.Consumption.MESSAGE;
                payloadType = firstMethodParamTypeAssignable.getType(0);
            } else {
                consumption = MediatorConfiguration.Consumption.PAYLOAD;
                payloadType = parameterTypes[0];
            }

            return new ValidationOutput(production, consumption, payloadType);
        }

        if (ClassUtils.isAssignable(returnType, Uni.class)) {
            // Case 3 or 4 - Uni variants
            // Expected parameter 1, Message or payload
            if (parameterTypes.length != 1) {
                throw ex.definitionOnParam("@Incoming", methodAsString, "Uni");
            }
            if (strict && returnTypeAssignable.check(Void.class, 0) != GenericTypeAssignable.Result.Assignable) {
                throw ex.definitionCompletionStageOfVoid(methodAsString);
            }

            MediatorConfiguration.Consumption consumption;
            Type payloadType;
            // Distinction between 3 and 4
            if (ClassUtils.isAssignable(parameterTypes[0], Message.class)) {
                consumption = MediatorConfiguration.Consumption.MESSAGE;
                payloadType = firstMethodParamTypeAssignable.getType(0);
            } else {
                consumption = MediatorConfiguration.Consumption.PAYLOAD;
                payloadType = parameterTypes[0];
            }

            if (payloadType == null) {
                log.unableToExtractIngestedPayloadType(methodAsString,
                        "Cannot extract the type from the method signature");
            }

            return new ValidationOutput(production, consumption, payloadType);
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
                throw ex.unsupportedSynchronousSignature(methodAsString);
            }

            if (strict && !(returnType.equals(Void.class) || returnType.equals(Void.TYPE))) {
                throw ex.definitionReturnVoid(methodAsString, returnType.getName());
            }

            return new ValidationOutput(production, consumption, param);
        }

        throw ex.definitionUnsupportedSignature("@Incoming", methodAsString);
    }

    private ValidationOutput validatePublisher() {
        final MediatorConfiguration.Consumption consumption = MediatorConfiguration.Consumption.NONE;

        // Supported signatures:
        // 1. Flow.Publisher<Message<O>> method(), Publisher<Message<O>>, PublisherBuilder<Message<O>>
        // 2. Flow.Publisher<O> method(), Publisher<O>, PublisherBuilder<O>
        // 5. O method() O cannot be Void
        // 6. Message<O> method()
        // 7. CompletionStage<Message<O>> method(), Uni<Message<O>>
        // 8. CompletionStage<O> method(), Uni<O>

        if (returnType == Void.TYPE) {
            throw ex.definitionNotVoid("@Outgoing", methodAsString);
        }

        if (parameterTypes.length != 0) {
            throw ex.definitionNoParametersExpected("@Outgoing", methodAsString);
        }

        if (ClassUtils.isAssignable(returnType, Flow.Publisher.class)
                || ClassUtils.isAssignable(returnType, Publisher.class)
                || ClassUtils.isAssignable(returnType, PublisherBuilder.class)) {
            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw ex.definitionMustDeclareParam("@Outgoing", methodAsString, returnType.getSimpleName());
            }

            // Case 1 or 2
            return new ValidationOutput(
                    assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                            ? MediatorConfiguration.Production.STREAM_OF_MESSAGE
                            : MediatorConfiguration.Production.STREAM_OF_PAYLOAD,
                    consumption,
                    ClassUtils.isAssignable(returnType, PublisherBuilder.class),
                    ClassUtils.isAssignable(returnType, Publisher.class),
                    null);
        }

        if (ClassUtils.isAssignable(returnType, Message.class)) {
            // Case 6
            return new ValidationOutput(MediatorConfiguration.Production.INDIVIDUAL_MESSAGE, consumption, null);
        }

        if (ClassUtils.isAssignable(returnType, CompletionStage.class)) {
            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw ex.definitionMustDeclareParam("@Outgoing", methodAsString, "CompletionStage");
            }

            // Case 7 and 8
            return new ValidationOutput(
                    assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                            ? MediatorConfiguration.Production.COMPLETION_STAGE_OF_MESSAGE
                            : MediatorConfiguration.Production.COMPLETION_STAGE_OF_PAYLOAD,
                    consumption, null);
        }

        if (ClassUtils.isAssignable(returnType, Uni.class)) {
            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw ex.definitionMustDeclareParam("@Outgoing", methodAsString, "Uni");
            }

            // Case 7 and 8 -> Uni variant
            return new ValidationOutput(
                    assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                            ? MediatorConfiguration.Production.UNI_OF_MESSAGE
                            : MediatorConfiguration.Production.UNI_OF_PAYLOAD,
                    consumption, null);
        }

        // Case 5
        return new ValidationOutput(MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD, consumption, null);
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
        boolean useBuilderTypes = false;
        boolean useReactiveStreams = false;
        Type payloadType;

        if (ClassUtils.isAssignable(returnType, Flow.Processor.class)
                || ClassUtils.isAssignable(returnType, Processor.class)
                || ClassUtils.isAssignable(returnType, ProcessorBuilder.class)) {
            // Case 1, 2 or 3, 4

            if (parameterTypes.length != 0) {
                throw ex.definitionMustNotHaveParams("@Incoming & @Outgoing", methodAsString);
            }
            GenericTypeAssignable.Result firstGenericParamOfReturn = returnTypeAssignable.check(Message.class, 0);
            if (firstGenericParamOfReturn == GenericTypeAssignable.Result.NotGeneric) {
                throw ex.definitionExpectedTwoParams("@Incoming & @Outgoing", methodAsString);
            }
            consumption = firstGenericParamOfReturn == GenericTypeAssignable.Result.Assignable
                    ? MediatorConfiguration.Consumption.STREAM_OF_MESSAGE
                    : MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD;

            if (consumption == MediatorConfiguration.Consumption.STREAM_OF_MESSAGE) {
                payloadType = returnTypeAssignable.getType(0, 0);
            } else {
                payloadType = returnTypeAssignable.getType(0);
            }

            GenericTypeAssignable.Result secondGenericParamOfReturn = returnTypeAssignable.check(Message.class, 1);
            if (secondGenericParamOfReturn == GenericTypeAssignable.Result.NotGeneric) {
                throw ex.definitionExpectedTwoParams("@Incoming & @Outgoing", methodAsString);
            }
            production = secondGenericParamOfReturn == GenericTypeAssignable.Result.Assignable
                    ? MediatorConfiguration.Production.STREAM_OF_MESSAGE
                    : MediatorConfiguration.Production.STREAM_OF_PAYLOAD;

            useBuilderTypes = ClassUtils.isAssignable(returnType, ProcessorBuilder.class);
            useReactiveStreams = ClassUtils.isAssignable(returnType, Processor.class);

        } else if (ClassUtils.isAssignable(returnType, Flow.Publisher.class)
                || ClassUtils.isAssignable(returnType, Publisher.class)
                || ClassUtils.isAssignable(returnType, PublisherBuilder.class)) {
            // Case 5, 6, 7, 8
            if (parameterTypes.length != 1) {
                throw ex.illegalArgumentForValidateProcessor(methodAsString);
            }

            GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
            if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                throw ex.definitionExpectedReturnedParam("@Outgoing", methodAsString, "Publisher");
            }
            production = assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                    ? MediatorConfiguration.Production.STREAM_OF_MESSAGE
                    : MediatorConfiguration.Production.STREAM_OF_PAYLOAD;

            consumption = ClassUtils.isAssignable(parameterTypes[0], Message.class)
                    ? MediatorConfiguration.Consumption.MESSAGE
                    : MediatorConfiguration.Consumption.PAYLOAD;

            payloadType = extractIngestedTypeFromFirstParameter(consumption, firstMethodParamTypeAssignable.getType(0),
                    parameterTypes[0]);

            useBuilderTypes = ClassUtils.isAssignable(returnType, PublisherBuilder.class);
            useReactiveStreams = ClassUtils.isAssignable(returnType, Publisher.class);
        } else {
            // Case 9, 10, 11, 12
            Class<?> param = parameterTypes[0];

            if (ClassUtils.isAssignable(returnType, CompletionStage.class)) {
                // Case 11 or 12
                GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
                if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                    throw ex.definitionExpectedReturnedParam("@Incoming & @Outgoing", methodAsString,
                            "CompletionStage");
                }

                production = assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                        ? MediatorConfiguration.Production.COMPLETION_STAGE_OF_MESSAGE
                        : MediatorConfiguration.Production.COMPLETION_STAGE_OF_PAYLOAD;
                consumption = ClassUtils.isAssignable(param, Message.class) ? MediatorConfiguration.Consumption.MESSAGE
                        : MediatorConfiguration.Consumption.PAYLOAD;

                payloadType = extractIngestedTypeFromFirstParameter(consumption,
                        firstMethodParamTypeAssignable.getType(0), param);
            } else if (ClassUtils.isAssignable(returnType, Uni.class)) {
                // Case 11 or 12 - Uni variant
                GenericTypeAssignable.Result assignableToMessageCheck = returnTypeAssignable.check(Message.class, 0);
                if (assignableToMessageCheck == GenericTypeAssignable.Result.NotGeneric) {
                    throw ex.definitionExpectedReturnedParam("@Incoming & @Outgoing", methodAsString, "Uni");
                }

                production = assignableToMessageCheck == GenericTypeAssignable.Result.Assignable
                        ? MediatorConfiguration.Production.UNI_OF_MESSAGE
                        : MediatorConfiguration.Production.UNI_OF_PAYLOAD;
                consumption = ClassUtils.isAssignable(param, Message.class) ? MediatorConfiguration.Consumption.MESSAGE
                        : MediatorConfiguration.Consumption.PAYLOAD;

                payloadType = extractIngestedTypeFromFirstParameter(consumption,
                        firstMethodParamTypeAssignable.getType(0), param);
            } else {
                // Case 9 or 10
                production = ClassUtils.isAssignable(returnType, Message.class)
                        ? MediatorConfiguration.Production.INDIVIDUAL_MESSAGE
                        : MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD;
                consumption = ClassUtils.isAssignable(param, Message.class) ? MediatorConfiguration.Consumption.MESSAGE
                        : MediatorConfiguration.Consumption.PAYLOAD;

                payloadType = extractIngestedTypeFromFirstParameter(consumption,
                        firstMethodParamTypeAssignable.getType(0), param);
            }
        }

        if (production == MediatorConfiguration.Production.INDIVIDUAL_MESSAGE
                && acknowledgment == Acknowledgment.Strategy.POST_PROCESSING) {
            throw ex.illegalStateForValidateProcessor(methodAsString);
        }

        return new ValidationOutput(production, consumption, useBuilderTypes, useReactiveStreams, payloadType);
    }

    private Type extractIngestedTypeFromFirstParameter(MediatorConfiguration.Consumption consumption,
            Type genericTypeOfFirstParam, Class<?> parameterType) {
        Type payloadType;
        if (consumption == MediatorConfiguration.Consumption.MESSAGE
                || consumption == MediatorConfiguration.Consumption.STREAM_OF_MESSAGE) {
            payloadType = genericTypeOfFirstParam;
        } else {
            payloadType = parameterType;
        }
        return payloadType;
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
        boolean useBuilderTypes;
        boolean useReactiveStreams;
        Type payloadType;

        // The mediator produces and consumes a stream
        GenericTypeAssignable.Result returnTypeGenericCheck = returnTypeAssignable.check(Message.class, 0);
        if (returnTypeGenericCheck == GenericTypeAssignable.Result.NotGeneric) {
            throw ex.definitionExpectedReturnedParam("@Outgoing", methodAsString, returnType.getSimpleName());
        }
        production = returnTypeGenericCheck == GenericTypeAssignable.Result.Assignable
                ? MediatorConfiguration.Production.STREAM_OF_MESSAGE
                : MediatorConfiguration.Production.STREAM_OF_PAYLOAD;

        GenericTypeAssignable.Result firstParamTypeGenericCheck = firstMethodParamTypeAssignable
                .check(Message.class, 0);
        if (firstParamTypeGenericCheck == GenericTypeAssignable.Result.NotGeneric) {
            throw ex.definitionExpectedConsumedParam("@Incoming", methodAsString, parameterTypes[0].getSimpleName());
        }
        consumption = firstParamTypeGenericCheck == GenericTypeAssignable.Result.Assignable
                ? MediatorConfiguration.Consumption.STREAM_OF_MESSAGE
                : MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD;

        // Post Acknowledgement is not supported
        if (acknowledgment == Acknowledgment.Strategy.POST_PROCESSING) {
            throw ex.definitionAutoAckNotSupported("@Incoming & @Outgoing", methodAsString);
        }

        // Validate method and be sure we are not in the case 2 and 4.
        if (consumption == MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD
                && (acknowledgment == Acknowledgment.Strategy.MANUAL)) {
            throw ex.definitionManualAckNotSupported("@Incoming & @Outgoing", methodAsString);
        }

        if (production == MediatorConfiguration.Production.STREAM_OF_PAYLOAD
                && acknowledgment == Acknowledgment.Strategy.MANUAL) {
            throw ex.definitionManualAckNotSupported("@Incoming & @Outgoing", methodAsString);
        }

        if (consumption == MediatorConfiguration.Consumption.STREAM_OF_MESSAGE) {
            payloadType = firstMethodParamTypeAssignable.getType(0, 0);
        } else {
            payloadType = firstMethodParamTypeAssignable.getType(0);
        }

        // Ensure that the parameter is also using the MP Reactive Streams Operator types.
        boolean builderParameter = ClassUtils.isAssignable(parameterTypes[0], PublisherBuilder.class);
        boolean builderReturn = ClassUtils.isAssignable(returnType, PublisherBuilder.class);
        if (builderParameter == builderReturn) {
            useBuilderTypes = builderParameter;
        } else {
            throw ex.definitionProduceConsume("@Incoming & @Outgoing", methodAsString, PublisherBuilder.class.getSimpleName());
        }

        // Ensure that the parameter is also using the ReactiveStreams type.
        boolean rsParameter = ClassUtils.isAssignable(parameterTypes[0], Publisher.class);
        boolean rsReturn = ClassUtils.isAssignable(returnType, Publisher.class);
        if (rsParameter == rsReturn) {
            useReactiveStreams = rsParameter;
        } else {
            throw ex.definitionProduceConsume("@Incoming & @Outgoing", methodAsString, Publisher.class.getSimpleName());
        }

        if (payloadType == null) {
            log.unableToExtractIngestedPayloadType(methodAsString, "Cannot extract the type from the method signature");
        }

        return new ValidationOutput(production, consumption, useBuilderTypes, useReactiveStreams, payloadType);
    }

    public Acknowledgment.Strategy processDefaultAcknowledgement(Shape shape,
            MediatorConfiguration.Consumption consumption, MediatorConfiguration.Production production) {
        if (shape == Shape.STREAM_TRANSFORMER) {
            if (production == MediatorConfiguration.Production.STREAM_OF_PAYLOAD) {
                return Acknowledgment.Strategy.PRE_PROCESSING;
            } else if (consumption == MediatorConfiguration.Consumption.STREAM_OF_PAYLOAD) {
                return Acknowledgment.Strategy.PRE_PROCESSING;
            } else {
                return Acknowledgment.Strategy.MANUAL;
            }
        } else if (shape == Shape.PROCESSOR) {
            if (consumption == MediatorConfiguration.Consumption.PAYLOAD) {
                if (production == MediatorConfiguration.Production.STREAM_OF_PAYLOAD
                        || production == MediatorConfiguration.Production.STREAM_OF_MESSAGE) {
                    return Acknowledgment.Strategy.PRE_PROCESSING;
                }
                return Acknowledgment.Strategy.POST_PROCESSING;
            } else if (consumption == MediatorConfiguration.Consumption.MESSAGE
                    || consumption == MediatorConfiguration.Consumption.STREAM_OF_MESSAGE) {
                return Acknowledgment.Strategy.MANUAL;
            } else {
                return Acknowledgment.Strategy.PRE_PROCESSING;
            }
        } else if (shape == Shape.SUBSCRIBER) {
            if (consumption == MediatorConfiguration.Consumption.STREAM_OF_MESSAGE
                    || consumption == MediatorConfiguration.Consumption.MESSAGE) {
                return Acknowledgment.Strategy.MANUAL;
            } else {
                return Acknowledgment.Strategy.POST_PROCESSING;
            }
        } else {
            return Acknowledgment.Strategy.POST_PROCESSING;
        }
    }

    public Merge.Mode processMerge(List<?> incomings, Supplier<Merge.Mode> supplier) {
        Merge.Mode result = supplier.get();
        if (incomings != null && !incomings.isEmpty()) {
            return result;
        } else if (result != null) {
            throw ex.definitionMergeOnlyIncoming("@Outgoing", methodAsString);
        }
        return null;
    }

    public Integer processBroadcast(Object outgoing, Supplier<Integer> supplier) {
        Integer result = supplier.get();
        if (outgoing != null) {
            return result;
        } else if (result != null) {
            throw ex.definitionBroadcastOnlyOutgoing("@Incoming", methodAsString);
        }
        return null;
    }

    public void validateBlocking(ValidationOutput validationOutput) {
        if (!(validationOutput.production.equals(MediatorConfiguration.Production.INDIVIDUAL_MESSAGE)
                || validationOutput.production.equals(MediatorConfiguration.Production.INDIVIDUAL_PAYLOAD)
                || validationOutput.production.equals(MediatorConfiguration.Production.NONE))) {
            throw ex.definitionBlockingOnlyIndividual(methodAsString);
        }

        if (!(validationOutput.consumption.equals(MediatorConfiguration.Consumption.MESSAGE)
                || validationOutput.consumption.equals(MediatorConfiguration.Consumption.PAYLOAD)
                || validationOutput.consumption.equals(MediatorConfiguration.Consumption.NONE))) {
            throw ex.definitionBlockingOnlyIndividualParam(methodAsString);
        }

        if (ClassUtils.isAssignable(returnType, CompletionStage.class)) {
            Type returnTypeParameter = returnTypeAssignable.getType(0);
            if (returnTypeParameter != null && !returnTypeParameter.getTypeName().equals(Void.class.getName())) {
                throw ex.definitionBlockingOnlyIndividual(methodAsString);
            }
        }

        if (ClassUtils.isAssignable(returnType, Uni.class)) {
            Type returnTypeParameter = returnTypeAssignable.getType(0);
            if (returnTypeParameter != null && !returnTypeParameter.getTypeName().equals(Void.class.getName())) {
                throw ex.definitionBlockingOnlyIndividual(methodAsString);
            }
        }
    }

    public void strict() {
        this.strict = true;
    }

    public static class ValidationOutput {
        private final MediatorConfiguration.Production production;
        private final MediatorConfiguration.Consumption consumption;
        private final boolean useBuilderTypes;
        private final boolean useReactiveStreams;
        private final Type ingestedPayloadType;

        public ValidationOutput(MediatorConfiguration.Production production,
                MediatorConfiguration.Consumption consumption, Type ingestedPayloadType) {
            this(production, consumption, false, false, ingestedPayloadType);
        }

        public ValidationOutput(MediatorConfiguration.Production production,
                MediatorConfiguration.Consumption consumption,
                boolean useBuilderTypes, boolean useReactiveStreams, Type ingestedPayloadType) {
            this.production = production;
            this.consumption = consumption;
            this.useBuilderTypes = useBuilderTypes;
            this.useReactiveStreams = useReactiveStreams;
            this.ingestedPayloadType = ingestedPayloadType;
        }

        public MediatorConfiguration.Production getProduction() {
            return production;
        }

        public MediatorConfiguration.Consumption getConsumption() {
            return consumption;
        }

        public boolean getUseBuilderTypes() {
            return useBuilderTypes;
        }

        public Type getIngestedPayloadType() {
            return ingestedPayloadType;
        }

        public boolean getUseReactiveStreams() {
            return useReactiveStreams;
        }
    }

    public interface GenericTypeAssignable {

        Result check(Class<?> target, int index);

        /**
         * Gets the underlying type. For example, on a {@code Message<X>}, it returns {@code X}.
         *
         * @param index the index of the type
         * @return the type, {@code null} if not set or wildcard
         */
        Type getType(int index);

        /**
         * Gets the underlying sub-type. For example, on a {@code Publisher<Message<X>>}, it returns {@code X}.
         *
         * @param index the index of the type
         * @param subIndex the second index
         * @return the type, {@code null} if not set or wildcard
         */
        Type getType(int index, int subIndex);

        enum Result {
            NotGeneric,
            InvalidIndex,
            NotAssignable,
            Assignable,
        }
    }
}
