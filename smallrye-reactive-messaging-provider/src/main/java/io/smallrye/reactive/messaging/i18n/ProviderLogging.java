package io.smallrye.reactive.messaging.i18n;

import java.util.List;
import java.util.Set;

import javax.enterprise.inject.spi.Bean;

import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "SRRML")
public interface ProviderLogging {

    // 00200-00299 (logging)

    ProviderLogging log = Logger.getMessageLogger(ProviderLogging.class, "io.smallrye.reactive.messaging");

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 200, value = "The method %s has thrown an exception")
    void methodException(String methodAsString, @Cause Throwable cause);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 201, value = "Error caught during the stream processing")
    void streamProcessingException(@Cause Throwable cause);

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

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 206, value = "Scanning Type: %s")
    void scanningType(Class javaClass);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 207, value = "Cancel subscriptions")
    void cancelSubscriptions();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 208, value = "Deployment done... start processing")
    void deploymentDoneStartProcessing();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 209, value = "Initializing mediators")
    void initializingMediators();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 210, value = "Initializing %s")
    void initializingMethod(String methodAsString);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 211, value = "Unable to create invoker instance of %s")
    void unableToCreateInvoker(Class invokerClass, @Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 212, value = "Unable to initialize mediator: %s")
    void unableToInitializeMediator(String methodAsString, @Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 213, value = "Registering %s as publisher %s")
    void registeringAsPublisher(String methodAsString, String outgoing);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 214, value = "Registering %s as subscriber %s")
    void registeringAsSubscriber(String methodAsString, List<String> list);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 215, value = "Connecting mediators")
    void connectingMediators();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 216, value = "Attempt to resolve %s")
    void attemptToResolve(String methodAsString);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 217, value = "Connecting %s to `%s` (%s)")
    void connectingTo(String methodAsString, List<String> list, PublisherBuilder publisher);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 218, value = "Connecting %s to `%s`")
    void connectingTo(String methodAsString, List<String> list);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 219, value = "Impossible to bind mediators, some mediators are not connected: %s \n Available publishers: %s \n Available emitters: %s")
    void impossibleToBindMediators(List<String> list, Set<String> incomingNames, Set<String> emitterNames);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 220, value = "Connecting method %s to sink %s")
    void connectingMethodToSink(String methodAsString, String name);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 221, value = "%d subscribers consuming the stream %s")
    void numberOfSubscribersConsumingStream(int size, String name);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 222, value = "Connecting emitter to sink %s")
    void connectingEmitterToSink(String name);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 223, value = "Mediator created for %s")
    void mediatorCreated(String methodAsString);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 224, value = "Analyzing mediator bean: %s")
    void analyzingMediatorBean(Bean bean);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 225, value = "No subscriber for channel %s  attached to the emitter %s.%s")
    void noSubscriberForChannelAttachedToEmitter(String name, String beanClassName, String memberName);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 226, value = "Found incoming connectors: %s")
    void foundIncomingConnectors(List<String> connectors);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 227, value = "Found incoming connectors: %s")
    void foundOutgoingConnectors(List<String> connectors);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 228, value = "No MicroProfile Config found, skipping")
    void skippingMPConfig();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 229, value = "Channel manager initializing...")
    void channelManagerInitializing();

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 230, value = "Unable to create the publisher or subscriber during initialization")
    void unableToCreatePublisherOrSubscriber(@Cause Throwable t);

}