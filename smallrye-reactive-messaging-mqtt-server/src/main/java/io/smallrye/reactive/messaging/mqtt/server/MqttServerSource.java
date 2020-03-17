package io.smallrye.reactive.messaging.mqtt.server;

import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.EXACTLY_ONCE;
import static io.vertx.core.net.NetServerOptions.DEFAULT_HOST;
import static io.vertx.core.net.NetworkOptions.DEFAULT_RECEIVE_BUFFER_SIZE;
import static io.vertx.core.net.TCPSSLOptions.DEFAULT_SSL;
import static io.vertx.mqtt.MqttServerOptions.DEFAULT_MAX_MESSAGE_SIZE;
import static io.vertx.mqtt.MqttServerOptions.DEFAULT_PORT;
import static io.vertx.mqtt.MqttServerOptions.DEFAULT_TIMEOUT_ON_CONNECT;
import static io.vertx.mqtt.MqttServerOptions.DEFAULT_TLS_PORT;

import java.util.concurrent.CompletableFuture;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.processors.BehaviorProcessor;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.mqtt.MqttServer;

class MqttServerSource {

    private final Logger logger = LoggerFactory.getLogger(MqttServerSource.class);
    private final boolean broadcast;
    private final PublisherBuilder<MqttMessage> source;
    private final MqttServer mqttServer;

    private static MqttServerOptions mqttServerOptions(Config config) {
        final MqttServerOptions options = new MqttServerOptions();
        options.setAutoClientId(config.getOptionalValue("auto-client-id", Boolean.class).orElse(true));
        options.setSsl(
                config.getOptionalValue("ssl", Boolean.class).orElse(DEFAULT_SSL));
        // TODO set KeyCertOptions if SSL, c.f. https://vertx.io/docs/vertx-mqtt/java/#_handling_client_connection_disconnection_with_ssl_tls_support
        options.setMaxMessageSize(config.getOptionalValue("max-message-size", Integer.class)
                .orElse(DEFAULT_MAX_MESSAGE_SIZE));
        options.setTimeoutOnConnect(config.getOptionalValue("timeout-on-connect", Integer.class)
                .orElse(DEFAULT_TIMEOUT_ON_CONNECT));
        options.setReceiveBufferSize(config.getOptionalValue("receive-buffer-size", Integer.class)
                .orElse(DEFAULT_RECEIVE_BUFFER_SIZE));
        final int defaultPort = options.isSsl() ? DEFAULT_TLS_PORT : DEFAULT_PORT;
        options.setPort(config.getOptionalValue("port", Integer.class).orElse(defaultPort));
        options.setHost(
                config.getOptionalValue("host", String.class).orElse(DEFAULT_HOST));
        return options;
    }

    MqttServerSource(Vertx vertx, Config config) {
        this.broadcast = config.getOptionalValue("broadcast", Boolean.class).orElse(false);
        final MqttServerOptions options = mqttServerOptions(config);
        this.mqttServer = MqttServer.create(vertx, options);
        final BehaviorProcessor<MqttMessage> processor = BehaviorProcessor.create();

        mqttServer.exceptionHandler(error -> {
            logger.error("Exception thrown", error);
            processor.onError(error);
        });

        mqttServer.endpointHandler(endpoint -> {
            logger.debug("MQTT client [{}] request to connect, clean session = {}",
                    endpoint.clientIdentifier(), endpoint.isCleanSession());

            if (endpoint.auth() != null) {
                logger.trace("[username = {}, password = {}]", endpoint.auth().getUsername(),
                        endpoint.auth().getPassword());
            }
            if (endpoint.will() != null) {
                logger.trace("[will topic = {} msg = {} QoS = {} isRetain = {}]",
                        endpoint.will().getWillTopic(), endpoint.will().getWillMessageBytes(),
                        endpoint.will().getWillQos(), endpoint.will().isWillRetain());
            }

            logger.trace("[keep alive timeout = {}]", endpoint.keepAliveTimeSeconds());

            endpoint.exceptionHandler(
                    error -> logger.error("Error with client " + endpoint.clientIdentifier(), error));

            endpoint.disconnectHandler(
                    v -> logger.debug("MQTT client [{}] disconnected", endpoint.clientIdentifier()));

            endpoint.pingHandler(
                    v -> logger.trace("Ping received from client [{}]", endpoint.clientIdentifier()));

            endpoint.publishHandler(message -> {
                logger.debug("Just received message [{}] with QoS [{}] from client [{}]",
                        message.payload(),
                        message.qosLevel(), endpoint.clientIdentifier());

                processor.onNext(new MqttMessage(message, endpoint.clientIdentifier(), () -> {
                    if (message.qosLevel() == AT_LEAST_ONCE) {
                        logger.trace("Send PUBACK to client [{}] for message [{}]",
                                endpoint.clientIdentifier(),
                                message.messageId());
                        endpoint.publishAcknowledge(message.messageId());
                    } else if (message.qosLevel() == EXACTLY_ONCE) {
                        logger.trace("Send PUBREC to client [{}] for message [{}]",
                                endpoint.clientIdentifier(),
                                message.messageId());
                        endpoint.publishReceived(message.messageId());
                    }
                    return CompletableFuture.completedFuture(null);
                }));
            });

            endpoint.publishReleaseHandler(messageId -> {
                logger.trace("Send PUBCOMP to client [{}] for message [{}]", endpoint.clientIdentifier(),
                        messageId);
                endpoint.publishComplete(messageId);
            });

            endpoint.subscribeHandler(subscribeMessage -> {
                logger.trace("Received subscription message {} from client [{}], closing connection",
                        subscribeMessage, endpoint.clientIdentifier());
                endpoint.close();
            });

            // accept connection from the remote client
            // this implementation doesn't keep track of sessions
            endpoint.accept(false);
        });

        this.source = ReactiveStreams.fromPublisher(processor
                .delaySubscription(mqttServer.listen()
                        .onItem().invoke(ignored -> logger
                                .info("MQTT server listening on {}:{}", options.getHost(), mqttServer.actualPort()))
                        .onFailure().invoke(throwable -> logger.error("Failed to start MQTT server", throwable))
                        .toMulti()
                        .then(flow -> {
                            if (broadcast) {
                                return flow.broadcast().toAllSubscribers();
                            } else {
                                return flow;
                            }
                        }))
                .doOnSubscribe(subscription -> logger.debug("New subscriber added {}", subscription)));
    }

    synchronized PublisherBuilder<MqttMessage> source() {
        return source;
    }

    synchronized void close() {
        mqttServer.close()
                .onFailure().invoke(t -> logger.warn("An exception has been caught while closing the MQTT server", t))
                .onItem().invoke(x -> logger.debug("MQTT server closed"))
                .onFailure().recoverWithItem((Void) null)
                .await().indefinitely();
    }

    synchronized int port() {
        return mqttServer.actualPort();
    }
}
