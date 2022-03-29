package io.smallrye.reactive.messaging.providers.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG", length = 5)
public interface ProviderMessages {

    ProviderMessages msg = Messages.getBundle(ProviderMessages.class);

    // 00100-00199 (messaging)

    @Message(id = 100, value = "Invoker not initialized")
    String invokerNotInitialized();

    @Message(id = 101, value = "Worker pool not initialized")
    String workerPoolNotInitialized();

    @Message(id = 102, value = "'name' must be set")
    String nameMustBeSet();

    @Message(id = 103, value = "Exception thrown when calling the method %s")
    String methodCallingExceptionMessage(String method);

    @Message(id = 104, value = "The method %s returned `null`")
    String methodReturnedNull(String methodAsString);

    @Message(id = 105, value = "Synchronous error caught during the subscription of `%s`")
    String weavingSynchronousError(String sources);

    @Message(id = 106, value = "Unable to connect stream `%s` (%s) - several publishers are available (%d), use the @Merge annotation to indicate the merge strategy.")
    String weavingUnableToConnect(String source, String method, int number);

    @Message(id = 107, value = "Action to execute not provided")
    String actionNotProvided();

    @Message(id = 108, value = "Worker Name not specified")
    String workerNameNotSpecified();

    @Message(id = 109, value = "Method was empty")
    String methodWasEmpty();

    @Message(id = 110, value = "className was empty")
    String classNameWasEmpty();

    @Message(id = 111, value = "AnnotatedType was empty")
    String annotatedTypeWasEmpty();

    @Message(id = 113, value = "%s is null")
    String isNull(String name);

    @Message(id = 117, value = "the prefix must not be set")
    String prefixMustNotBeSet();

    @Message(id = 118, value = "the config must not be set")
    String configMustNotBeSet();

    @Message(id = 119, value = "the channel name must be set")
    String channelMustNotBeSet();

    @Message(id = 120, value = "'stream' must be set")
    String streamMustBeSet();

    @Message(id = 121, value = "'subscriber' must be set")
    String subscriberMustBeSet();

    @Message(id = 122, value = "'emitter' must be set")
    String emitterMustBeSet();

    @Message(id = 123, value = "'method' must be set")
    String methodMustBeSet();

    @Message(id = 124, value = "'bean' must be set")
    String beanMustBeSet();
}
