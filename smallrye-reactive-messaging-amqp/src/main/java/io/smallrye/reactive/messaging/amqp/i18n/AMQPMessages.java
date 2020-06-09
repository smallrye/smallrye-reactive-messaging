package io.smallrye.reactive.messaging.amqp.i18n;

import java.lang.reflect.Type;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for AMQP Connector
 * Assigned ID range is 16100-16199
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface AMQPMessages {

    AMQPMessages msg = Messages.getBundle(AMQPMessages.class);

    @Message(id = 16100, value = "Invoker not initialized")
    String invokerNotInitialized();

    @Message(id = 16101, value = "Worker pool not initialized")
    String workerPoolNotInitialized();

    @Message(id = 16102, value = "'%s' must be set")
    String mustBeSet(String name);

    @Message(id = 16103, value = "Exception thrown when calling the method %s")
    String methodCallingExceptionMessage(String method);

    @Message(id = 16104, value = "The method %s returned `null`")
    String methodReturnedNull(String methodAsString);

    @Message(id = 16105, value = "Synchronous error caught during the subscription of `%s`")
    String weavingSynchronousError(String sources);

    @Message(id = 16106, value = "Unable to connect stream `%s` (%s) - several publishers are available (%d), use the @Merge annotation to indicate the merge strategy.")
    String weavingUnableToConnect(String source, String method, int number);

    @Message(id = 16107, value = "Action to execute not provided")
    String actionNotProvided();

    @Message(id = 16108, value = "Worker Name not specified")
    String workerNameNotSpecified();

    @Message(id = 16109, value = "Method was empty")
    String methodWasEmpty();

    @Message(id = 16110, value = "className was empty")
    String classNameWasEmpty();

    @Message(id = 16111, value = "AnnotatedType was empty")
    String annotatedTypeWasEmpty();

    @Message(id = 16112, value = "null value specified for bounds array")
    String nullSpecifiedForBounds();

    @Message(id = 16113, value = "%s is null")
    String isNull(String name);

    @Message(id = 16114, value = "no owner allowed for top-level %s")
    String noOwnerAllowed(Class<?> raw);

    @Message(id = 16115, value = "%s is invalid owner type for parameterized %s")
    String invalidOwnerForParameterized(Type owner, Class<?> raw);

    @Message(id = 16116, value = "invalid number of type parameters specified: expected %d, got %d")
    String invalidNumberOfTypeParameters(int parameterLength, int argumentLength);

    @Message(id = 16117, value = "the prefix must not be set")
    String prefixMustNotBeSet();

    @Message(id = 16118, value = "the config must not be set")
    String configMustNotBeSet();

    @Message(id = 16119, value = "the channel name must be set")
    String channelMustNotBeSet();
}