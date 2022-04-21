package io.smallrye.reactive.messaging.providers.i18n;

import java.util.Set;

import jakarta.enterprise.inject.spi.Bean;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.Once;

import io.smallrye.reactive.messaging.providers.wiring.Wiring;

@MessageLogger(projectCode = "SRMSG", length = 5)
public interface ProviderLogging extends BasicLogger {

    // 00200-00299 (logging)

    ProviderLogging log = Logger.getMessageLogger(ProviderLogging.class, "io.smallrye.reactive.messaging.provider");

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 200, value = "The method %s has thrown an exception")
    void methodException(String methodAsString, @Cause Throwable cause);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 201, value = "Error caught while processing a message")
    void messageProcessingException(@Cause Throwable cause);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 202, value = "Created new Vertx instance")
    void vertXInstanceCreated();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 203, value = "Created worker pool named %s with concurrency of %d")
    void workerPoolCreated(String workerName, Integer count);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 204, value = "Multiple publisher found for %s, using the merge policy `ONE` takes the first found")
    void multiplePublisherFound(String source);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 205, value = "Strict mode enabled")
    void strictModeEnabled();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 206, value = "Scanning Type: %s")
    void scanningType(Class<?> javaClass);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 207, value = "%s")
    void reportWiringFailures(String message);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 208, value = "The connector '%s' has no downstreams")
    void connectorWithoutDownstream(Wiring.Component component);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 209, value = "Beginning graph resolution, number of components detected: %d")
    void startGraphResolution(int size);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 210, value = "Graph resolution completed in %d ns")
    void completedGraphResolution(long duration);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 211, value = "Unable to create invoker instance of %s")
    void unableToCreateInvoker(Class<?> invokerClass, @Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 212, value = "Unable to initialize mediator: %s")
    void unableToInitializeMediator(String methodAsString, @Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 224, value = "Analyzing mediator bean: %s")
    void analyzingMediatorBean(Bean<?> bean);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 226, value = "Found incoming connectors: %s")
    void foundIncomingConnectors(Set<String> connectors);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 227, value = "Found outgoing connectors: %s")
    void foundOutgoingConnectors(Set<String> connectors);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 228, value = "No MicroProfile Config found, skipping")
    void skippingMPConfig();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 229, value = "Channel manager initializing...")
    void channelManagerInitializing();

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 230, value = "Unable to create the publisher or subscriber during initialization")
    void unableToCreatePublisherOrSubscriber(@Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 231, value = "Incoming channel `%s` disabled by configuration")
    void incomingChannelDisabled(String channel);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 232, value = "Outgoing channel `%s` disabled by configuration")
    void outgoingChannelDisabled(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 233, value = "Unable to extract the ingested payload type for method `%s`, the reason is: %s")
    void unableToExtractIngestedPayloadType(String method, String reason);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 234, value = "Failed to emit a Message to the channel")
    void failureEmittingMessage(@Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 235, value = "Beginning materialization")
    void startMaterialization();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 236, value = "Materialization completed in %d ns")
    void materializationCompleted(long duration);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 237, value = "Use of @jakarta.inject.Named in Reactive Messaging is deprecated, use @io.smallrye.common.annotation.Identifier instead")
    @Once
    void deprecatedNamed();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 238, value = "No ExecutionHolder, disabling @Blocking support")
    void noExecutionHolderDisablingBlockingSupport();
}
