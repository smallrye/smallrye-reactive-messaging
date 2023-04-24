package io.smallrye.reactive.messaging.rabbitmq.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.Logger.Level;
import org.jboss.logging.annotations.*;

@MessageLogger(projectCode = "SRMSG", length = 5)
public interface RabbitMQLogging extends BasicLogger {

    RabbitMQLogging log = Logger.getMessageLogger(RabbitMQLogging.class, "io.smallrye.reactive.messaging.rabbitmq");

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17000, value = "RabbitMQ Receiver listening address %s")
    void receiverListeningAddress(String address);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17001, value = "RabbitMQ Receiver error")
    void receiverError(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17002, value = "Unable to retrieve messages from RabbitMQ, retrying...")
    void retrieveMessagesRetrying(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17003, value = "Unable to retrieve messages from RabbitMQ, no more retry")
    void retrieveMessagesNoMoreRetrying(@Cause Throwable t);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17006, value = "Establishing connection with RabbitMQ broker for channel `%s`")
    void establishingConnection(String channel);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17007, value = "Connection with RabbitMQ broker established for channel `%s`")
    void connectionEstablished(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17008, value = "Unable to connect to the broker, retry will be attempted")
    void unableToConnectToBroker(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17009, value = "Unable to recover from RabbitMQ connection disruption")
    void unableToRecoverFromConnectionDisruption(@Cause Throwable t);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 17010, value = "A message sent to channel `%s` has been nacked, ignoring the failure and marking the RabbitMQ message as accepted")
    void nackedAcceptMessage(String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17011, value = "The full ignored failure is")
    void fullIgnoredFailure(@Cause Throwable t);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17012, value = "A message sent to channel `%s` has been nacked, rejecting the RabbitMQ message and fail-stop")
    void nackedFailMessage(String channel);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 17013, value = "A message sent to channel `%s` has been nacked, ignoring the failure and marking the RabbitMQ message as rejected")
    void nackedIgnoreMessage(String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17018, value = "Failure reported for channel `%s`, closing client")
    void failureReported(String channel, @Cause Throwable reason);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17021, value = "Unable to serialize message on channel `%s`, message has been nacked")
    void serializationFailure(String channel, @Cause Throwable reason);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17022, value = "Sending a message to exchange `%s` with routing key %s")
    void sendingMessageToExchange(String exchange, String routingKey);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17023, value = "Established exchange `%s`")
    void exchangeEstablished(String exchangeName);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17024, value = "Unable to establish exchange `%s`")
    void unableToEstablishExchange(String exchangeName, @Cause Throwable ex);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17025, value = "Established queue `%s`")
    void queueEstablished(String queueName);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17026, value = "Unable to bind queue '%s' to exchange '%s'")
    void unableToEstablishBinding(String queueName, String exchangeName, @Cause Throwable ex);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17027, value = "Established binding of queue `%s` to exchange '%s' using routing key '%s' and arguments '%s'")
    void bindingEstablished(String queueName, String exchangeName, String routingKey, String arguments);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17028, value = "Unable to establish queue `%s`")
    void unableToEstablishQueue(String exchangeName, @Cause Throwable ex);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17029, value = "Established dlx `%s`")
    void dlxEstablished(String deadLetterExchangeName);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17030, value = "Unable to establish dlx `%s`")
    void unableToEstablishDlx(String deadLetterExchangeName, @Cause Throwable ex);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17033, value = "A message sent to channel `%s` has been ack'd")
    void ackMessage(String channel);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17034, value = "A message sent to channel `%s` has not been explicitly ack'd as auto-ack is enabled")
    void ackAutoMessage(String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17035, value = "Creating RabbitMQ client from bean named '%s'")
    void createClientFromBean(String optionsBeanName);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17036, value = "RabbitMQ broker configured to %s:%d for channel %s")
    void brokerConfigured(String host, int port, String channel);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17037, value = "Unable to create client")
    void unableToCreateClient(@Cause Throwable t);

    @Once
    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 17038, value = "No valid content_type set, failing back to byte[]. If that's wanted, set the content type to application/octet-stream with \"content-type-override\"")
    void typeConversionFallback();

    @LogMessage(level = Level.DEBUG)
    @Message(id = 17039, value = "Connection '%d' with RabbitMQ broker established for channel `%s`")
    void connectionEstablished(int connectionIndex, String channel);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17040, value = "Established dead letter binding of queue `%s` to exchange '%s' using routing key '%s'")
    void deadLetterBindingEstablished(String queueName, String exchangeName, String routingKey);

}
