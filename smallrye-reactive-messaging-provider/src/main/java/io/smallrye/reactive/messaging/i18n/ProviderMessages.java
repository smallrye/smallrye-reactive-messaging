package io.smallrye.reactive.messaging.i18n;

import java.lang.reflect.Type;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRRML")
public interface ProviderMessages {

    ProviderMessages msg = Messages.getBundle(ProviderMessages.class);

    // 00100-00199 (messaging)

    @Message(id = 100, value = "Invoker not initialized")
    String invokerNotInitialized();

    @Message(id = 101, value = "Worker pool not initialized")
    String workerPoolNotInitialized();

    @Message(id = 102, value = "'%s' must be set")
    String mustBeSet(String name);

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

    @Message(id = 112, value = "null value specified for bounds array")
    String nullSpecifiedForBounds();

    @Message(id = 113, value = "%s is null")
    String isNull(String name);

    @Message(id = 114, value = "no owner allowed for top-level %s")
    String noOwnerAllowed(Class<?> raw);

    @Message(id = 115, value = "%s is invalid owner type for parameterized %s")
    String invalidOwnerForParameterized(Type owner, Class<?> raw);

    @Message(id = 116, value = "invalid number of type parameters specified: expected %d, got %d")
    String invalidNumberOfTypeParameters(int parameterLength, int argumentLength);

    @Message(id = 117, value = "the prefix must not be set")
    String prefixMustNotBeSet();

    @Message(id = 118, value = "the config must not be set")
    String configMustNotBeSet();

    @Message(id = 119, value = "the channel name must be set")
    String channelMustNotBeSet();
}