package io.smallrye.reactive.messaging.mqtt.server.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.reactivestreams.Subscription;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.mqtt.messages.MqttSubscribeMessage;

/**
 * Logging for MQTT Server Connector
 * Assigned ID range is 17500-17599
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface MqttServerLogging extends BasicLogger {

    MqttServerLogging log = Logger.getMessageLogger(MqttServerLogging.class, "io.smallrye.reactive.messaging.mqtt-server");

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17500, value = "Exception thrown")
    void exceptionThrown(@Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17501, value = "MQTT client [%s] request to connect, clean session = %s")
    void requestToConnect(String clientIdentifier, boolean isClean);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 17502, value = "[username = %s, password = %s]")
    void requestToConnectUserName(String user, String pass);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 17503, value = "[will topic = %s msg = %s QoS = %s isRetain = %s]")
    void requestToConnectWill(String topic, byte[] msg, int qos, boolean retain);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 17504, value = "[keep alive timeout = %s]")
    void requestToConnectKeepAlive(int s);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17505, value = "Error with client %s")
    void errorWithClient(String clientIdentifier, @Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17506, value = "MQTT client [%s] disconnected")
    void clientDisconnected(String clientIdentifier);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 17507, value = "Ping received from client [%s]")
    void pingReceived(String clientIdentifier);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17508, value = "Just received message [%s] with QoS [%s] from client [%s]")
    void receivedMessageFromClient(Buffer buffer, MqttQoS qos, String client);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 17509, value = "Send %s to client [%s] for message [%s]")
    void sendToClient(String f, String client, int messageId);

    @LogMessage(level = Logger.Level.TRACE)
    @Message(id = 17510, value = "Received subscription message %s from client [%s], closing connection")
    void receivedSubscription(MqttSubscribeMessage message, String client);

    @LogMessage(level = Logger.Level.INFO)
    @Message(id = 17511, value = "MQTT server listening on %s:%s")
    void serverListeningOn(String host, int port);

    @LogMessage(level = Logger.Level.ERROR)
    @Message(id = 17512, value = "Failed to start MQTT server")
    void failedToStart(@Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17513, value = "New subscriber added %s")
    void newSubscriberAdded(Subscription subscription);

    @LogMessage(level = Logger.Level.WARN)
    @Message(id = 17514, value = "An exception has been caught while closing the MQTT server")
    void exceptionWhileClosing(@Cause Throwable t);

    @LogMessage(level = Logger.Level.DEBUG)
    @Message(id = 17515, value = "MQTT server closed")
    void closed();
}
