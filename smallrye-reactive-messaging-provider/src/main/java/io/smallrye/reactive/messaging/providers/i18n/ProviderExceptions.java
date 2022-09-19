package io.smallrye.reactive.messaging.providers.i18n;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import jakarta.enterprise.inject.spi.DefinitionException;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.enterprise.inject.spi.InjectionPoint;

import org.eclipse.microprofile.config.spi.Converter;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.providers.ProcessingException;
import io.smallrye.reactive.messaging.providers.WeavingException;

@MessageBundle(projectCode = "SRMSG", length = 5)
public interface ProviderExceptions {

    ProviderExceptions ex = Messages.getBundle(ProviderExceptions.class);

    // 00000-00099 (exceptions)

    @Message(id = 0, value = "%s")
    ProcessingException processingException(String methodAsString, @Cause Throwable cause);

    @Message(id = 1, value = "Method %s only has %d so parameter with index %d cannot be retrieved")
    IllegalArgumentException illegalArgumentForGenericParameterType(Method method, int length, int index);

    @Message(id = 2, value = "Invalid method annotated with %s: %s - value is blank or null")
    IllegalArgumentException illegalArgumentForAnnotationNullOrBlank(String annotation, String annotationTarget);

    @Message(id = 3, value = "Unknown shape: %s")
    IllegalStateException illegalStateExceptionForValidate(Shape shape);

    @Message(id = 4, value = "Invalid method annotated with @Outgoing and @Incoming %s - one parameter expected")
    IllegalArgumentException illegalArgumentForValidateProcessor(String methodAsString);

    @Message(id = 5, value = "Unsupported acknowledgement policy - POST_PROCESSING not supported when producing messages for %s")
    IllegalStateException illegalStateForValidateProcessor(String methodAsString);

    @Message(id = 6, value = "Invalid method annotated with %s: %s - The @Acknowledgment annotation is only supported for method annotated with @Incoming")
    DefinitionException definitionExceptionUnsupported(String annotation, String methodAsString);

    @Message(id = 7, value = "Unsupported shape %s for method %s")
    IllegalArgumentException illegalArgumentForUnsupportedShape(Shape shape, String methodAsString);

    @Message(id = 8, value = "Expected a Processor shape, received a %s")
    IllegalArgumentException illegalArgumentForProcessorShape(Shape shape);

    @Message(id = 9, value = "Invalid Processor - unsupported signature for %s")
    IllegalArgumentException illegalArgumentForInitialize(String methodAsString);

    @Message(id = 10, value = "Unexpected production type: %s")
    IllegalArgumentException illegalArgumentForUnexpectedProduction(MediatorConfiguration.Production production);

    @Message(id = 11, value = "Expected a Publisher shape, received a %s")
    IllegalArgumentException illegalArgumentForPublisherShape(Shape shape);

    @Message(id = 12, value = "Unexpected consumption type: %s")
    IllegalArgumentException illegalArgumentForUnexpectedConsumption(MediatorConfiguration.Consumption consumption);

    @Message(id = 13, value = "Expected a Subscriber shape, received a %s")
    IllegalArgumentException illegalArgumentForSubscriberShape(Shape shape);

    @Message(id = 14, value = "%s")
    WeavingException weavingForIncoming(List<String> incoming, @Cause Throwable cause);

    @Message(id = 15, value = "Invalid return type: %s - expected a Subscriber or a SubscriberBuilder")
    IllegalStateException illegalStateExceptionForSubscriberOrSubscriberBuilder(String resultClassName);

    @Message(id = 16, value = "Failed to create Worker for %s")
    RuntimeException runtimeForFailedWorker(String workerName);

    @Message(id = 17, value = "@Blocking referred to invalid worker name.")
    IllegalArgumentException illegalArgumentForFailedWorker();

    @Message(id = 18, value = "Unable to find a stream with the name %s, available streams are: %s")
    IllegalStateException illegalStateForStream(String name, Set<String> valid);

    @Message(id = 19, value = "Unable to connect an emitter with the channel `%s`")
    DefinitionException incomingNotFoundForEmitter(String name);

    @Message(id = 20, value = "Missing @Channel qualifier for + `%s`")
    DefinitionException emitterWithoutChannelAnnotation(InjectionPoint injectionPoint);

    @Message(id = 21, value = "The default buffer size must be strictly positive")
    IllegalArgumentException illegalArgumentForDefaultBuffer();

    @Message(id = 22, value = "Invalid back-pressure strategy: %s")
    IllegalArgumentException illegalArgumentForBackPressure(OnOverflow.Strategy overFlowStrategy);

    @Message(id = 23, value = "`null` is not a valid value")
    IllegalArgumentException illegalArgumentForNullValue();

    @Message(id = 24, value = "The emitter encountered a failure")
    IllegalStateException incomingNotFoundForEmitter(@Cause Throwable throwable);

    @Message(id = 25, value = "The downstream has cancelled the consumption")
    IllegalStateException illegalStateForDownstreamCancel();

    @Message(id = 26, value = "The emitter encountered a failure while emitting")
    IllegalStateException illegalStateForEmitterWhileEmitting(@Cause Throwable throwable);

    @Message(id = 27, value = "Cannot send a message, there is no subscriber found for the channel '%s'. " +
            "Before calling `send`, you can verify there is a subscriber and demands using `emitter.hasRequests()`. " +
            "Alternatively, you can add `@OnOverflow(OnOverflow.Strategy.DROP)` on the emitter.")
    IllegalStateException noEmitterForChannel(String name);

    @Message(id = 28, value = "The subscription to %s has been cancelled")
    IllegalStateException illegalStateForCancelledSubscriber(String name);

    @Message(id = 29, value = "`%s` is not a valid exception")
    IllegalArgumentException illegalArgumentForException(String val);

    @Message(id = 34, value = "Insufficient downstream requests to emit item")
    IllegalStateException illegalStateInsufficientDownstreamRequests();

    @Message(id = 35, value = "found an unhandled type: %s")
    IllegalStateException illegalStateUnhandledType(Type type);

    @Message(id = 36, value = "missing assignment type for type variable %s")
    IllegalArgumentException illegalArgumentMissingAssignment(Type type);

    @Message(id = 37, value = "Unexpected generic interface type found: %s")
    IllegalStateException illegalStateUnexpectedGenericInterface(Type type);

    @Message(id = 38, value = "%s")
    IllegalArgumentException illegalArgumentTypeToString(String typeName);

    @Message(id = 39, value = "`name` must be set")
    IllegalArgumentException nameMustBeSet();

    @Message(id = 40, value = "%s must not be `null`")
    IllegalArgumentException validationForNotNull(String name);

    @Message(id = 41, value = "%s must not be `empty`")
    IllegalArgumentException validationForNotEmpty(String name);

    @Message(id = 42, value = "%s must not contain a `null` element")
    IllegalArgumentException validationForContainsNull(String name);

    @Message(id = 43, value = "%s")
    IllegalArgumentException validateIsTrue(String value);

    @Message(id = 44, value = "Invalid channel configuration -  the `channel-name` attribute cannot be used in configuration (channel `%s`)")
    IllegalArgumentException illegalArgumentInvalidChannelConfiguration(String name);

    @Message(id = 45, value = "Cannot find attribute `%s` for channel `%s`. Has been tried: % and %s")
    NoSuchElementException noSuchElementForAttribute(String propertyName, String name, String channelKey, String connectorKey);

    @Message(id = 46, value = "%ss must contain a non-empty array of %s")
    IllegalArgumentException illegalArgumentForAnnotationNonEmpty(String annotation, String annotationTarget);

    @Message(id = 47, value = "Invalid method annotated with %s: %s - when returning a Subscriber or a SubscriberBuilder, no parameters are expected")
    DefinitionException definitionNoParamOnSubscriber(String annotation, String methodAsString);

    @Message(id = 48, value = "Invalid method annotated with %s: %s - the returned Subscriber must declare a type parameter")
    DefinitionException definitionSubscriberTypeParam(String annotation, String methodAsString);

    @Message(id = 49, value = "Invalid method annotated with %s: %s - when returning a %s, one parameter is expected")
    DefinitionException definitionOnParam(String annotation, String methodAsString, String returnType);

    @Message(id = 50, value = "Invalid method annotated with %s: %s - Unsupported signature")
    DefinitionException definitionUnsupportedSignature(String annotation, String methodAsString);

    @Message(id = 51, value = "Invalid method annotated with @Incoming: %s - The signature is not supported. The method consumes a `Message`, so the returned type must be `CompletionStage<Void>` or `Uni<Void>`.")
    DefinitionException unsupportedSynchronousSignature(String methodAsString);

    @Message(id = 52, value = "Invalid method annotated with %s: %s - the method must not be `void`")
    DefinitionException definitionNotVoid(String annotation, String methodAsString);

    @Message(id = 53, value = "Invalid method annotated with %s: %s - no parameters expected")
    DefinitionException definitionNoParametersExpected(String annotation, String methodAsString);

    @Message(id = 54, value = "Invalid method annotated with %s: %s - the returned %s must declare a type parameter")
    DefinitionException definitionMustDeclareParam(String annotation, String methodAsString, String returnClass);

    @Message(id = 55, value = "Invalid method annotated with %s: %s - the method must not have parameters")
    DefinitionException definitionMustNotHaveParams(String annotation, String methodAsString);

    @Message(id = 56, value = "Invalid method annotated with %s: %s - Expected 2 type parameters for the returned Processor")
    DefinitionException definitionExpectedTwoParams(String annotation, String methodAsString);

    @Message(id = 57, value = "Invalid method annotated with %s: %s - Expected a type parameter in the returned %s")
    DefinitionException definitionExpectedReturnedParam(String annotation, String methodAsString, String returnClass);

    @Message(id = 58, value = "Invalid method annotated with %s: %s - Expected a type parameter for the consumed  %s")
    DefinitionException definitionExpectedConsumedParam(String annotation, String methodAsString, String returnClass);

    @Message(id = 59, value = "Invalid method annotated with %s: %s - Automatic post-processing acknowledgment is not supported.")
    DefinitionException definitionAutoAckNotSupported(String annotation, String methodAsString);

    @Message(id = 60, value = "Invalid method annotated with %s: %s - Consuming a stream of payload is not supported with MANUAL acknowledgment. Use a Publisher<Message<I>> or PublisherBuilder<Message<I>> instead.")
    DefinitionException definitionManualAckNotSupported(String annotation, String methodAsString);

    @Message(id = 61, value = "Invalid method annotated with %s: %s - If the method produces a PublisherBuilder, it needs to consume a PublisherBuilder.")
    DefinitionException definitionProduceConsume(String annotation, String methodAsString);

    @Message(id = 62, value = "Invalid method annotated with %s: %s - The @Merge annotation is only supported for method annotated with @Incoming")
    DefinitionException definitionMergeOnlyIncoming(String annotation, String methodAsString);

    @Message(id = 63, value = "Invalid method annotated with %s: %s - The @Broadcast annotation is only supported for method annotated with @Outgoing")
    DefinitionException definitionBroadcastOnlyOutgoing(String annotation, String methodAsString);

    @Message(id = 64, value = "Invalid method annotated with @Blocking: %s - The @Blocking annotation is only supported for methods returning `void` (@Incoming only), a `Message` or a payload")
    DefinitionException definitionBlockingOnlyIndividual(String methodAsString);

    @Message(id = 65, value = "Invalid method annotated with @Blocking: %s - The @Blocking annotation is only supported for methods consuming an individual Message or payload like `consume(String s)` or `consume(Message<String> s)")
    DefinitionException definitionBlockingOnlyIndividualParam(String methodAsString);

    @Message(id = 66, value = "Invalid method annotated with @Blocking: %s - no @Incoming or @Outgoing present")
    IllegalArgumentException illegalBlockingSignature(String methodAsString);

    @Message(id = 67, value = "Invalid method annotated with %s: %s - %s was not defined")
    IllegalArgumentException illegalArgumentForWorkerConfigKey(String annotation, String annotationTarget,
            String workerConfigKey);

    @Message(id = 68, value = "The operation %s has returned null")
    NullPointerException nullPointerOnInvokeBlocking(String methodAsString);

    @Message(id = 71, value = "Invalid channel configuration -  the `connector` attribute must be set for channel `%s`")
    IllegalArgumentException illegalArgumentChannelConnectorConfiguration(String name);

    @Message(id = 72, value = "Unknown connector for `%s`.")
    IllegalArgumentException illegalArgumentUnknownConnector(String name);

    @Message(id = 73, value = "Invalid configuration, the following channel names cannot be used for both incoming and outgoing: %s")
    DeploymentException deploymentInvalidConfiguration(Set<String> sources);

    @Message(id = 74, value = "Unable to retrieve the config")
    IllegalStateException illegalStateRetrieveConfig();

    @Message(id = 75, value = "Invalid Emitter injection found for `%s`. Injecting an `Emitter<Message<T>>` is invalid. You can use an `Emitter<T>` to send instances of `T` and `Message<T>`.")
    DefinitionException invalidEmitterOfMessage(InjectionPoint ip);

    @Message(id = 76, value = "Invalid Emitter injection found for  `%s`. The Emitter expected to be parameterized with the emitted type, such as Emitter<String>.")
    DefinitionException invalidRawEmitter(InjectionPoint ip);

    @Message(id = 77, value = "No converter for type `%s`")
    NoSuchElementException noConverterForType(Class<?> propertyType);

    @Message(id = 78, value = "Converter `%s` returned null for value `%s`")
    NoSuchElementException converterReturnedNull(Converter<?> converter, String value);

    @Message(id = 79, value = "The config is not of type `%s`")
    IllegalArgumentException configNotOfType(Class<?> type);

    @Message(id = 80, value = "Invalid method annotated with @Incoming: %s - when returning a CompletionStage, you must return a CompletionStage<Void>")
    DefinitionException definitionCompletionStageOfVoid(String methodAsString);

    @Message(id = 81, value = "Invalid method annotated with @Incoming: %s. The signature is not supported as the produced result would be ignored. "
            + "The method must return `void`, found %s.")
    DefinitionException definitionReturnVoid(String methodAsString, String returnType);

    @Message(id = 82, value = "Invalid method signature for %s - method returning a Multi<Message<O>>, Publisher<Message<O>> or a PublisherBuilder<Message<O>> cannot consume an individual payload. You must consume a Message<I> instead and handle the acknowledgement.")
    DefinitionException definitionProduceMessageStreamAndConsumePayload(String methodAsString);

    @Message(id = 83, value = "Invalid method signature for %s - method returning a Multi<O>, Publisher<O> or a PublisherBuilder<O> cannot consume an individual message. You must consume a payload instead or return a Publisher<Message<O>> or a PublisherBuilder<Message<O>>.")
    DefinitionException definitionProducePayloadStreamAndConsumeMessage(String methodAsString);

    @Message(id = 84, value = "Invalid method signature for %s - method returning a Multi<O>, Publisher<O> or a PublisherBuilder<O> cannot consume a Publisher<Message<I>> or a Multi<Message<I>>. You must consume a Multi<I> or a Publisher<I> instead.")
    DefinitionException definitionProducePayloadStreamAndConsumeMessageStream(String messageAsString);

    @Message(id = 85, value = "Invalid method signature for %s - method returning a Multi<Message<O>>, Publisher<Message<O>> or a PublisherBuilder<Message<O>> cannot consume a Publisher<I> or a Multi<I>. You must consume a Multi<Message<I>> or a Publisher<Message<I>> instead and handle the acknowledgement.")
    DefinitionException definitionProduceMessageStreamAndConsumePayloadStream(String messageAsString);

    @Message(id = 86, value = "Multiple beans expose the connector %s : %s, %s")
    DeploymentException multipleBeanDeclaration(String connector, String bean1, String bean2);

    @Message(id = 87, value = "The bean %s implements a connector interface but does not use the @Connector qualifier")
    DefinitionException missingConnectorQualifier(String clazz);
}
