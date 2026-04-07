/*
 * Copyright 2021 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.smallrye.reactive.messaging.mqtt.session;

import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.reactive.messaging.mqtt.session.impl.MqttClientSessionImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.messages.MqttAuthenticationExchangeMessage;
import io.vertx.mqtt.messages.MqttConnAckMessage;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.codes.MqttAuthenticateReasonCode;

/**
 * An MQTT client session.
 */
public interface MqttClientSession {

    /**
     * Create a new MQTT client session.
     * <p>
     * The session will initially be disconnected, and must be started using {@link #start()}.
     *
     * @param vertx Vert.x instance
     * @param options MQTT client session options
     * @return MQTT client session instance
     */
    static MqttClientSession create(Vertx vertx, MqttClientSessionOptions options) {
        return new MqttClientSessionImpl(vertx, options);
    }

    /**
     * Get the last CONNACK message received from the server.
     * <p>
     * This provides access to MQTT v5 CONNACK properties like assigned client identifier,
     * server keep alive, maximum QoS, etc.
     *
     * @return The last CONNACK message, or {@code null} if not yet connected.
     */
    MqttConnAckMessage getConnAckMessage();

    /**
     * Set the session state handler.
     *
     * @param sessionStateHandler The new handler, will overwrite the old one.
     * @return current MQTT client session instance
     */
    MqttClientSession sessionStateHandler(Handler<SessionEvent> sessionStateHandler);

    /**
     * Set the subscription state handler.
     *
     * @param subscriptionStateHandler The new handler, will overwrite the old one.
     * @return current MQTT client session instance
     */
    MqttClientSession subscriptionStateHandler(Handler<SubscriptionEvent> subscriptionStateHandler);

    /**
     * Set the publish complete handler.
     *
     * @param publishCompleteHandler The new handler, will overwrite the old one.
     * @return current MQTT client session instance
     * @see MqttClient#publishCompletionHandler(Handler)
     */
    MqttClientSession publishCompletionHandler(Handler<Integer> publishCompleteHandler);

    /**
     * Set the publish completion expiration handler.
     *
     * @param publishCompletionExpirationHandler The new handler, will overwrite the old one.
     * @return current MQTT client session instance
     * @see MqttClient#publishCompletionExpirationHandler(Handler)
     */
    MqttClientSession publishCompletionExpirationHandler(Handler<Integer> publishCompletionExpirationHandler);

    /**
     * Set the publish completion unknown packet id handler.
     *
     * @param publishCompletionUnknownPacketIdHandler The new handler, will overwrite the old one.
     * @return current MQTT client session instance
     * @see MqttClient#publishCompletionUnknownPacketIdHandler(Handler)
     */
    MqttClientSession publishCompletionUnknownPacketIdHandler(Handler<Integer> publishCompletionUnknownPacketIdHandler);

    /**
     * Start the session. This will try to drive the connection to {@link SessionState#CONNECTED}.
     */
    Future<Void> start();

    /**
     * Stop the session. This will try to drive the connection to {@link SessionState#DISCONNECTED}.
     */
    Future<Void> stop();

    /**
     * Stop the session with a custom session expiry interval (MQTT 5.0).
     * <p>
     * The session expiry sent in the DISCONNECT packet overrides the value from CONNECT.
     * Use 0 to expire the session immediately, or a positive value to extend the session lifetime.
     *
     * @param sessionExpiryInterval the session expiry interval in seconds, or {@code null} to use the CONNECT value
     */
    default Future<Void> stop(Long sessionExpiryInterval) {
        return stop();
    }

    /**
     * Get the current session state.
     *
     * @return The current state.
     */
    SessionState getState();

    /**
     * Get a current subscription state.
     *
     * @param topicFilter The topic filter to get the state for.
     * @return The current state of the requested subscription.
     */
    SubscriptionState getSubscriptionState(String topicFilter);

    /**
     * Check if the session is currently connected.
     *
     * @return {@code true} if the session is currently connected, {@code false} otherwise.
     */
    default boolean isConnected() {
        return getState() == SessionState.CONNECTED;
    }

    /**
     * Subscribes to a single topic with related QoS level.
     *
     * @param topic The topic to subscribe to.
     * @param qos The QoS to request from the server.
     * @return current MQTT client session instance
     */
    Future<Integer> subscribe(String topic, RequestedQoS qos);

    /**
     * Subscribes to a single topic with MQTT v5 subscription options.
     *
     * @param topic The topic to subscribe to.
     * @param qos The QoS to request from the server.
     * @param noLocal If true, the server will not forward messages published by this client.
     * @param retainAsPublished If true, the server keeps the retain flag as set by the publishing client.
     * @param retainHandling Retain handling option (0, 1, or 2).
     * @return a future completed with the granted QoS
     */
    default Future<Integer> subscribe(String topic, RequestedQoS qos, boolean noLocal, boolean retainAsPublished,
            int retainHandling) {
        return subscribe(topic, qos, noLocal, retainAsPublished, retainHandling, null);
    }

    /**
     * Subscribes to a single topic with MQTT v5 subscription options and subscription identifier.
     *
     * @param topic The topic to subscribe to.
     * @param qos The QoS to request from the server.
     * @param noLocal If true, the server will not forward messages published by this client.
     * @param retainAsPublished If true, the server keeps the retain flag as set by the publishing client.
     * @param retainHandling Retain handling option (0, 1, or 2).
     * @param subscriptionIdentifier The subscription identifier (MQTT 5.0), or {@code null} to omit.
     * @return a future completed with the granted QoS
     */
    default Future<Integer> subscribe(String topic, RequestedQoS qos, boolean noLocal, boolean retainAsPublished,
            int retainHandling, Integer subscriptionIdentifier) {
        return subscribe(topic, qos);
    }

    /**
     * Unsubscribe from receiving messages on given topic
     *
     * @param topic Topic you want to unsubscribe from
     * @return current MQTT client session instance
     */
    Future<Void> unsubscribe(String topic);

    /**
     * Sets handler which will be called each time server publish something to client
     *
     * @param messageHandler handler to call
     * @return current MQTT client session instance
     */
    MqttClientSession messageHandler(Handler<MqttPublishMessage> messageHandler);

    /**
     * Sets handler which will be called in case of an exception
     *
     * @param exceptionHandler handler to call
     * @return current MQTT client session instance
     */
    MqttClientSession exceptionHandler(Handler<Throwable> exceptionHandler);

    /**
     * Sends the PUBLISH message to the remote MQTT server
     *
     * @param topic topic on which the message is published
     * @param payload message payload
     * @param qosLevel QoS level
     * @param isDup if the message is a duplicate
     * @param isRetain if the message needs to be retained
     * @return a {@code Future} completed after PUBLISH packet sent with packetid (not when QoS 0)
     */
    Future<Integer> publish(String topic, Buffer payload, MqttQoS qosLevel, boolean isDup, boolean isRetain);

    /**
     * Sends the PUBLISH message to the remote MQTT server
     *
     * @param topic topic on which the message is published
     * @param payload message payload
     * @param qosLevel QoS level
     * @return a {@code Future} completed after PUBLISH packet sent with packetid (not when QoS 0)
     */
    default Future<Integer> publish(String topic, Buffer payload, MqttQoS qosLevel) {
        return publish(topic, payload, qosLevel, false, false);
    }

    /**
     * Sends the PUBLISH message to the remote MQTT server with MQTT 5.0 properties.
     *
     * @param topic topic on which the message is published
     * @param payload message payload
     * @param qosLevel QoS level
     * @param isDup if the message is a duplicate
     * @param isRetain if the message needs to be retained
     * @param properties MQTT 5.0 properties
     * @return a {@code Future} completed after PUBLISH packet sent with packetid (not when QoS 0)
     */
    Future<Integer> publish(String topic, Buffer payload, MqttQoS qosLevel, boolean isDup, boolean isRetain,
            MqttProperties properties);

    /**
     * Sets handler which will be called after AUTH packet receiving (MQTT 5.0 enhanced authentication).
     *
     * @param handler handler to call with the authentication exchange message
     * @return current MQTT client session instance
     */
    MqttClientSession authenticationExchangeHandler(Handler<MqttAuthenticationExchangeMessage> handler);

    /**
     * Sends an AUTH packet to the server (MQTT 5.0 enhanced authentication).
     *
     * @param reasonCode the reason code for the AUTH packet
     * @param properties the properties for the AUTH packet
     * @return a future completed after AUTH packet is sent
     */
    Future<Void> authenticate(MqttAuthenticateReasonCode reasonCode, MqttProperties properties);
}
