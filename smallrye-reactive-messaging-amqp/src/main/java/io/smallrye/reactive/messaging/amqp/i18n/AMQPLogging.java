package io.smallrye.reactive.messaging.amqp.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for AMQP Connector
 * Assigned ID range is 16200-16299
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface AMQPLogging extends BasicLogger {

    AMQPLogging log = Logger.getMessageLogger(AMQPLogging.class, "io.smallrye.reactive.messaging.amqp");

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 16200, value = "Creating AMQP client from bean named '%s'")
    void createClientFromBean(String optionsBeanName);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 16201, value = "AMQP broker configured to %s:%d for channel %s")
    void brokerConfigured(String host, int port, String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16202, value = "Unable to create client")
    void unableToCreateClient(@Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 16203, value = "AMQP Receiver listening address %s")
    void receiverListeningAddress(String address);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16204, value = "AMQP Receiver error")
    void receiverError(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16205, value = "Unable to retrieve messages from AMQP, retrying...")
    void retrieveMessagesRetrying(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16206, value = "Unable to retrieve messages from AMQP, no more retry")
    void retrieveMessagesNoMoreRetrying(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16207, value = "The AMQP message has not been sent, the client is closed")
    void messageNotSendClientClosed();

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16208, value = "Unable to send the AMQP message")
    void unableToSendMessage(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 16209, value = "Unable to use the address configured in the message (%s) - the connector is not using an anonymous sender, using %s instead")
    void unableToUseAddress(String address, String configuredAddress);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16210, value = "The AMQP message to address `%s` has not been sent, the client is closed")
    void messageToAddressNotSend(String address);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 16211, value = "Sending AMQP message to address `%s`")
    void sendingMessageToAddress(String address);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 16212, value = "Establishing connection with AMQP broker")
    void establishingConnection();

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 16213, value = "Connection with AMQP broker established")
    void connectionEstablished();

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16214, value = "AMQP Connection failure")
    void connectionFailure(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16215, value = "Unable to connect to the broker, retry will be attempted")
    void unableToConnectToBroker(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16216, value = "Unable to recover from AMQP connection disruption")
    void unableToRecoverFromConnectionDisruption(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 16217, value = "A message sent to channel `%s` has been nacked, ignoring the failure and marking the AMQP message as accepted")
    void nackedAcceptMessage(String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 16218, value = "The full ignored failure is")
    void fullIgnoredFailure(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16219, value = "A message sent to channel `%s` has been nacked, rejecting the AMQP message and fail-stop")
    void nackedFailMessage(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 16220, value = "A message sent to channel `%s` has been nacked, ignoring the failure and marking the AMQP message as rejected")
    void nackedIgnoreMessage(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 16221, value = "A message sent to channel `%s` has been nacked, ignoring the failure and marking the AMQP message as released")
    void nackedReleaseMessage(String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 16222, value = "Retrieved credits for channel `%s`: %s")
    void retrievedCreditsForChannel(String channel, long credits);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 16223, value = "No more credit for channel %s, requesting more credits")
    void noMoreCreditsForChannel(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16224, value = "The AMQP message to address `%s` has not been sent, the client is closed")
    void messageNoSend(String actualAddress);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16225, value = "Failure reported for channel `%s`, closing client")
    void failureReported(String channel, @Cause Throwable reason);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 16226, value = "A message sent to channel `%s` has been nacked, ignoring the message and marking the AMQP message as modified with `delivery-failed`")
    void nackedModifiedFailedMessage(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 16227, value = "A message sent to channel `%s` has been nacked, ignoring the message and marking the AMQP message as modified with `delivery-failed` and `undeliverable-here`")
    void nackedModifiedFailedMessageAndUndeliverableHere(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16228, value = "Unable to serialize message on channel `%s`, message has been nacked")
    void serializationFailure(String channel, @Cause Throwable reason);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 16229, value = "Unable to deserialize AMQP message on channel `%s`, message ignored")
    void unableToCreateMessage(String channel, @Cause Exception e);
}
