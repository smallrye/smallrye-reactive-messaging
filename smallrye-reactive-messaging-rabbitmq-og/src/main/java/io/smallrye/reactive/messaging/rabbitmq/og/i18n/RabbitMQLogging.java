package io.smallrye.reactive.messaging.rabbitmq.og.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.Logger.Level;
import org.jboss.logging.annotations.*;

@MessageLogger(projectCode = "SRMSG", length = 5)
public interface RabbitMQLogging extends BasicLogger {

    RabbitMQLogging log = Logger.getMessageLogger(RabbitMQLogging.class, "io.smallrye.reactive.messaging.rabbitmq.og");

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18000, value = "RabbitMQ Receiver listening address %s")
    void receiverListeningAddress(String address);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18001, value = "RabbitMQ Receiver error")
    void receiverError(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18002, value = "Unable to retrieve messages from RabbitMQ, retrying...")
    void retrieveMessagesRetrying(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18003, value = "Unable to retrieve messages from RabbitMQ, no more retry")
    void retrieveMessagesNoMoreRetrying(@Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18006, value = "Establishing connection with RabbitMQ broker for channel `%s`")
    void establishingConnection(String channel);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18007, value = "Connection with RabbitMQ broker established for channel `%s`")
    void connectionEstablished(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18008, value = "Unable to connect to the broker, retry will be attempted")
    void unableToConnectToBroker(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18009, value = "Unable to recover from RabbitMQ connection disruption")
    void unableToRecoverFromConnectionDisruption(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18010, value = "A message sent to channel `%s` has been nacked, ignoring the failure and marking the RabbitMQ message as accepted")
    void nackedAcceptMessage(String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18011, value = "The full ignored failure is")
    void fullIgnoredFailure(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18012, value = "A message sent to channel `%s` has been nacked, rejecting the RabbitMQ message and fail-stop")
    void nackedFailMessage(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18013, value = "A message sent to channel `%s` has been nacked, ignoring the failure and marking the RabbitMQ message as rejected")
    void nackedIgnoreMessage(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18018, value = "Failure reported for channel `%s`, closing client")
    void failureReported(String channel, @Cause Throwable reason);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18021, value = "Unable to serialize message on channel `%s`, message has been nacked")
    void serializationFailure(String channel, @Cause Throwable reason);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18022, value = "Sending a message to exchange `%s` with routing key %s")
    void sendingMessageToExchange(String exchange, String routingKey);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18023, value = "Established exchange `%s`")
    void exchangeEstablished(String exchangeName);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18024, value = "Unable to establish exchange `%s`")
    void unableToEstablishExchange(String exchangeName, @Cause Throwable ex);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18025, value = "Established queue `%s`")
    void queueEstablished(String queueName);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18026, value = "Unable to bind queue '%s' to exchange '%s'")
    void unableToEstablishBinding(String queueName, String exchangeName, @Cause Throwable ex);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18027, value = "Established binding of queue `%s` to exchange '%s' using routing key '%s' and arguments '%s'")
    void bindingEstablished(String queueName, String exchangeName, String routingKey, String arguments);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18028, value = "Unable to establish queue `%s`")
    void unableToEstablishQueue(String exchangeName, @Cause Throwable ex);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18029, value = "Established dlx `%s`")
    void dlxEstablished(String deadLetterExchangeName);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18030, value = "Unable to establish dlx `%s`")
    void unableToEstablishDlx(String deadLetterExchangeName, @Cause Throwable ex);

    @LogMessage(level = Level.DEBUG)
    @Message(id = 18033, value = "A message sent to channel `%s` has been ack'd")
    void ackMessage(String channel);

    @LogMessage(level = Level.DEBUG)
    @Message(id = 18034, value = "A message sent to channel `%s` has not been explicitly ack'd as auto-ack is enabled")
    void ackAutoMessage(String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18035, value = "Creating RabbitMQ client from bean named '%s'")
    void createClientFromBean(String optionsBeanName);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18036, value = "RabbitMQ broker configured to %s for channel %s")
    void brokerConfigured(String address, String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18037, value = "Unable to create client")
    void unableToCreateClient(@Cause Throwable t);

    @Once
    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18038, value = "No valid content_type set, failing back to byte[]. If that's wanted, set the content type to application/octet-stream with \"content-type-override\"")
    void typeConversionFallback();

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18040, value = "Established dead letter binding of queue `%s` to exchange '%s' using routing key '%s'")
    void deadLetterBindingEstablished(String queueName, String exchangeName, String routingKey);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18041, value = "RabbitMQ connection recovered for channel `%s`")
    void connectionRecovered(String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18042, value = "Creating RabbitMQ channel for `%s`")
    void creatingChannel(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18043, value = "Error handling message on channel `%s`")
    void error(String channel, @Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18044, value = "QoS set to %d for channel `%s`")
    void qosSet(int prefetchCount, String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18045, value = "Topology established for channel `%s`, queue `%s`")
    void topologyEstablished(String channel, String queueName);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18046, value = "Consumer started for channel `%s`, queue `%s`, consumer tag `%s`")
    void consumerStarted(String channel, String queueName, String consumerTag);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 18047, value = "Message received on channel `%s`")
    void messageReceived(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18048, value = "Message processing failed on channel `%s`")
    void messageProcessingFailed(String channel, @Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18049, value = "Message conversion failed on channel `%s`")
    void messageConversionFailed(String channel, @Cause Throwable e);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 18050, value = "Consumer cancelled for channel `%s`, consumer tag `%s`")
    void consumerCancelled(String channel, String consumerTag);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18051, value = "Consumer shutdown for channel `%s`, consumer tag `%s`")
    void consumerShutdown(String channel, String consumerTag, @Cause Throwable sig);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18052, value = "Unable to create consumer for channel `%s`")
    void unableToCreateConsumer(String channel, @Cause Throwable e);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18053, value = "Unable to cancel consumer for channel `%s`")
    void unableToCancelConsumer(String channel, @Cause Throwable e);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18054, value = "Unable to close channel for `%s`")
    void unableToCloseChannel(String channel, @Cause Throwable e);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18055, value = "Cleanup failed for channel `%s`")
    void cleanupFailed(String channel, @Cause Throwable e);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18056, value = "Publisher confirms enabled for channel `%s`")
    void publisherConfirmsEnabled(String channel);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18057, value = "Publisher ready for channel `%s`")
    void publisherReady(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18058, value = "Unable to create publisher for channel `%s`")
    void unableToCreatePublisher(String channel, @Cause Throwable e);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18059, value = "Message publish failed for channel `%s`")
    void messagePublishFailed(String channel, @Cause Throwable throwable);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18060, value = "Publisher error for channel `%s`")
    void publisherError(String channel, @Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 18061, value = "Publisher complete for channel `%s`")
    void publisherComplete(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 18062, value = "Wait for confirms failed for channel `%s`")
    void waitForConfirmsFailed(String channel, @Cause Throwable e);
}
