package io.smallrye.reactive.messaging.mqtt.session.impl;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSession;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.mqtt.session.ReconnectDelayProvider;
import io.smallrye.reactive.messaging.mqtt.session.RequestedQoS;
import io.smallrye.reactive.messaging.mqtt.session.SessionEvent;
import io.smallrye.reactive.messaging.mqtt.session.SessionState;
import io.smallrye.reactive.messaging.mqtt.session.SubscriptionEvent;
import io.smallrye.reactive.messaging.mqtt.session.SubscriptionState;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.messages.MqttConnAckMessage;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;

public class MqttClientSessionImpl implements MqttClientSession {

    private static final Logger log = LoggerFactory.getLogger(MqttClientSessionImpl.class);

    private final VertxInternal vertx;
    private final MqttClientSessionOptions options;

    // record the subscriptions
    private final Map<String, RequestedQoS> subscriptions = new HashMap<>();
    // record the pending subscribes
    private final Map<Integer, LinkedHashMap<String, RequestedQoS>> pendingSubscribes = new HashMap<>();
    // record the pending unsubscribes
    private final Map<Integer, List<String>> pendingUnsubscribes = new HashMap<>();
    // the provider for the reconnect delay
    private final ReconnectDelayProvider reconnectDelay;

    // the current state
    private volatile SessionState state = SessionState.DISCONNECTED;
    // drives to connection either to CONNECTED or DISCONNECTED
    private volatile boolean running;
    // subscription states
    private final Map<String, SubscriptionState> subscriptionStates = new ConcurrentHashMap<>();

    // holds the actual MQTT client connection
    private MqttClient client;
    // an optional reconnect timer
    private Long reconnectTimer;

    private volatile Handler<MqttPublishMessage> messageHandler;
    private volatile Handler<Throwable> exceptionHandler;
    private volatile Handler<SessionEvent> sessionStateHandler;
    private volatile Handler<SubscriptionEvent> subscriptionStateHandler;
    private volatile Handler<Integer> publishCompleteHandler;
    private volatile Handler<Integer> publishCompletionExpirationHandler;
    private volatile Handler<Integer> publishCompletionUnknownPacketIdHandler;

    private final List<Handler<AsyncResult<Void>>> notifyConnected = new LinkedList<>();
    private final List<Handler<AsyncResult<Void>>> notifyStopped = new LinkedList<>();
    private final Map<String, List<Handler<AsyncResult<Integer>>>> notifySubscribed = new HashMap<>();
    private final Map<String, List<Handler<AsyncResult<Void>>>> notifyUnsubscribed = new HashMap<>();

    /**
     * Create a new instance, which is not started.
     *
     * @param vertx The vert.x instance to use.
     * @param options The client session options.
     */
    public MqttClientSessionImpl(final Vertx vertx, final MqttClientSessionOptions options) {
        this.vertx = (VertxInternal) vertx;
        this.options = options;
        this.reconnectDelay = options.getReconnectDelay().createProvider();
    }

    @Override
    public Future<Void> start() {
        Promise<Void> promise = Promise.promise();
        this.vertx.runOnContext(x -> doStart(promise));
        return promise.future();
    }

    @Override
    public Future<Void> stop() {
        Promise<Void> promise = Promise.promise();
        this.vertx.runOnContext(x -> doStop(promise));
        return promise.future();
    }

    @Override
    public SessionState getState() {
        return this.state;
    }

    @Override
    public SubscriptionState getSubscriptionState(String topicFilter) {
        return this.subscriptionStates.get(topicFilter);
    }

    @Override
    public Future<Integer> subscribe(String topic, RequestedQoS qos) {
        Promise<Integer> result = Promise.promise();
        this.vertx.runOnContext(x -> doSubscribe(topic, qos, result));
        return result.future();
    }

    @Override
    public Future<Void> unsubscribe(String topic) {
        Promise<Void> result = Promise.promise();
        this.vertx.runOnContext(x -> doUnsubscribe(topic, result));
        return result.future();
    }

    private void doStart(Handler<AsyncResult<Void>> handler) {
        if (this.running) {
            // nothing to do

            if (handler != null) {
                if (this.state == SessionState.CONNECTED) {
                    handler.handle(Future.succeededFuture());
                } else {
                    this.notifyConnected.add(handler);
                }
            }

            // return early
            return;
        }

        // remember to call back
        if (handler != null) {
            this.notifyConnected.add(handler);
        }

        // we connect, not re-connect
        this.reconnectDelay.reset();

        this.running = true;
        switch (this.state) {
            case DISCONNECTED:
                // initiate connection
                createConnection();
                break;
            case CONNECTING:
                // nothing to do
                break;
            case CONNECTED:
                // nothing to do
                break;
            case DISCONNECTING:
                // we do nothing here and wait until the disconnection advanced, which will then trigger a re-connect
                break;
        }
    }

    private void doStop(Handler<AsyncResult<Void>> handler) {
        if (!this.running) {
            // nothing to do

            if (handler != null) {
                if (this.state == SessionState.DISCONNECTED) {
                    handler.handle(Future.succeededFuture());
                } else {
                    this.notifyStopped.add(handler);
                }
            }

            return;
        }

        if (handler != null) {
            this.notifyStopped.add(handler);
        }

        this.running = false;

        if (this.reconnectTimer != null) {
            // we have a re-connect scheduled, but stop right now.
            this.vertx.cancelTimer(this.reconnectTimer);
        }

        switch (this.state) {
            case CONNECTED:
                closeConnection(new VertxException("Stop requested"));
                break;
            case DISCONNECTED:
                // nothing to do
                break;
            case DISCONNECTING:
                // nothing do do
                break;
            case CONNECTING:
                // we do nothing here and wait, until the connection advanced, which will then trigger a disconnect
                break;
        }
    }

    @Override
    public MqttClientSession exceptionHandler(Handler<Throwable> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    @Override
    public MqttClientSession sessionStateHandler(Handler<SessionEvent> sessionStateHandler) {
        this.sessionStateHandler = sessionStateHandler;
        return this;
    }

    @Override
    public MqttClientSession subscriptionStateHandler(Handler<SubscriptionEvent> subscriptionStateHandler) {
        this.subscriptionStateHandler = subscriptionStateHandler;
        return this;
    }

    @Override
    public MqttClientSession publishCompletionHandler(Handler<Integer> publishCompleteHandler) {
        this.publishCompleteHandler = publishCompleteHandler;
        return this;
    }

    @Override
    public MqttClientSession publishCompletionExpirationHandler(Handler<Integer> publishCompletionExpirationHandler) {
        this.publishCompletionExpirationHandler = publishCompletionExpirationHandler;
        return this;
    }

    @Override
    public MqttClientSession publishCompletionUnknownPacketIdHandler(Handler<Integer> publishCompletionUnknownPacketIdHandler) {
        this.publishCompletionUnknownPacketIdHandler = publishCompletionUnknownPacketIdHandler;
        return this;
    }

    @Override
    public MqttClientSession messageHandler(Handler<MqttPublishMessage> messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    /**
     * Set the state of the session.
     *
     * @param sessionState The new state.
     * @param cause The optional cause, in case of an error.
     */
    private void setState(final SessionState sessionState, final Throwable cause) {

        if (log.isDebugEnabled()) {
            log.debug(String.format("setState - current: %s, next: %s", this.state, sessionState), cause);
        }

        // before announcing our state change

        switch (sessionState) {
            case CONNECTING:
                break;
            case CONNECTED:
                // successful connection, reset delay
                this.reconnectDelay.reset();
                break;
            case DISCONNECTING:
                break;
            case DISCONNECTED:
                this.pendingUnsubscribes.clear();
                this.pendingSubscribes.clear();
                for (String topic : this.subscriptions.keySet()) {
                    notifySubscriptionState(topic, SubscriptionState.UNSUBSCRIBED, null);
                }
                break;
        }

        // announce state change

        if (this.state != sessionState) {
            this.state = sessionState;
            Handler<SessionEvent> handler = this.sessionStateHandler;
            if (handler != null) {
                handler.handle(new SessionEventImpl(sessionState, cause));
            }
        }

        // after announcing out state change

        switch (this.state) {
            case CONNECTING:
                // we just wait for the outcome
                break;
            case CONNECTED:
                if (!this.running) {
                    closeConnection((Throwable) null);
                } else {
                    // notify listeners
                    for (Handler<AsyncResult<Void>> handler : this.notifyConnected) {
                        handler.handle(Future.succeededFuture());
                    }
                    this.notifyConnected.clear();
                }
                break;
            case DISCONNECTING:
                // we just wait for the outcome
                break;
            case DISCONNECTED:
                if (this.running) {
                    scheduleReconnect();
                } else {
                    // notify listeners
                    for (Handler<AsyncResult<Void>> handler : this.notifyConnected) {
                        handler.handle(Future.failedFuture("Session stopped"));
                    }
                    this.notifyConnected.clear();
                    for (Handler<AsyncResult<Void>> handler : this.notifyStopped) {
                        handler.handle(Future.succeededFuture());
                    }
                    this.notifyStopped.clear();
                }
                break;
        }
    }

    private void notifySubscriptionState(final String topic, final SubscriptionState state, final Integer grantedQoS) {

        if (log.isDebugEnabled()) {
            log.debug(
                    String.format("notifySubscriptionState - topic: %s, state: %s, grantedQoS: %s", topic, state, grantedQoS));
        }

        this.subscriptionStates.put(topic, state);

        // send state event
        {
            Handler<SubscriptionEvent> handler = this.subscriptionStateHandler;
            if (handler != null) {
                handler.handle(new SubscriptionEventImpl(topic, state, grantedQoS));
            }
        }

        // notify waiting for a subscription
        if (state == SubscriptionState.SUBSCRIBED || state == SubscriptionState.FAILED) {
            List<Handler<AsyncResult<Integer>>> handlers = this.notifySubscribed.remove(topic);
            if (handlers != null) {
                for (Handler<AsyncResult<Integer>> handler : handlers) {
                    if (grantedQoS != null) {
                        handler.handle(Future.succeededFuture(grantedQoS));
                    } else {
                        handler.handle(Future.failedFuture("Unable to subscribe"));
                    }
                }
            }
        }

        // notify waiting for an unsubscription
        if (state == SubscriptionState.UNSUBSCRIBED) {
            List<Handler<AsyncResult<Void>>> handlers = this.notifyUnsubscribed.remove(topic);
            if (handlers != null) {
                for (Handler<AsyncResult<Void>> handler : handlers) {
                    if (grantedQoS != null) {
                        handler.handle(Future.succeededFuture());
                    } else {
                        handler.handle(Future.failedFuture("Unable to subscribe"));
                    }
                }
            }
        }

    }

    private void scheduleReconnect() {
        log.debug("Scheduling reconnect");

        if (this.reconnectTimer == null) {

            final Duration delay = nextDelay();
            if (log.isDebugEnabled()) {
                log.debug("Next delay: " + delay);
            }

            final long timer = vertx.setTimer(delay.toMillis(), x -> createConnection());
            if (log.isDebugEnabled()) {
                log.debug("Timer set: " + timer);
            }

            this.reconnectTimer = timer;
        }
    }

    /**
     * Calculate the next delay before trying to re-connect.
     *
     * @return The duration to wait.
     */
    private Duration nextDelay() {
        return this.reconnectDelay.nextDelay();
    }

    /**
     * Initiates the connection.
     */
    private void createConnection() {
        log.debug("Creating connection");

        // clear reconnect timer
        this.reconnectTimer = null;

        // create client
        this.client = MqttClient.create(this.vertx, this.options);
        this.client.exceptionHandler(this::exceptionCaught);
        this.client.closeHandler(x -> connectionClosed());
        this.client.publishHandler(this::serverPublished);
        this.client.subscribeCompletionHandler(this::subscribeCompleted);
        this.client.unsubscribeCompletionHandler(this::unsubscribeCompleted);
        this.client.publishCompletionHandler(this::publishComplete);
        this.client.publishCompletionExpirationHandler(this::publishExpired);
        this.client.publishCompletionUnknownPacketIdHandler(this::publishCompletionUnknown);

        // change state
        setState(SessionState.CONNECTING, null);
        // start connection
        this.client
                .connect(
                        this.options.getPort(),
                        this.options.getHostname(),
                        this.options.getServerName()
                                .orElse(this.options.getHostname()),
                        this::connectCompleted);
    }

    /**
     * Handle a caught exception.
     */
    private void exceptionCaught(Throwable cause) {
        log.debug("Caught exception", cause);
        closeConnection(cause);
        Handler<Throwable> exceptionHandler = this.exceptionHandler;
        if (exceptionHandler != null) {
            exceptionHandler.handle(cause);
        }
    }

    /**
     * Initiates the connection shutdown.
     */
    private void closeConnection(Throwable cause) {
        log.debug("Closing connection", cause);

        setState(SessionState.DISCONNECTING, cause);
        this.client.disconnect().onComplete(this::disconnectCompleted);
    }

    /**
     * Gets called when the connect call was processed.
     *
     * @param result The outcome of the connect call.
     */
    private void connectCompleted(AsyncResult<MqttConnAckMessage> result) {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Connect completed - result: %s, cause: %s", result.result(), result.cause()));
        }

        if (result.failed() || result.result() == null) {
            // this will include CONACKs with error codes
            setState(SessionState.DISCONNECTED, result.cause());
            return;
        }

        MqttConnAckMessage ack = result.result();

        setState(SessionState.CONNECTED, null);

        if (log.isDebugEnabled()) {
            log.debug(String.format("Subscriptions: %s, cleanSession: %s, sessionPresent: %s", !this.subscriptions.isEmpty(),
                    options.isCleanSession(), ack.isSessionPresent()));
        }

        if (options.isCleanSession() || !ack.isSessionPresent()) {
            // re-subscribe if we have requested subscriptions and (either cleanSession=true or no session found on the server)
            requestSubscribe(new LinkedHashMap<>(this.subscriptions));
        } else {
            // If the session is present on broker, I mark all subscription to SUBSC
            log.debug("Session present on broker, subscriptions request not sent. "
                    + "Be sure that the subscriptions on the broker side are the same that this client needs.");
            this.subscriptions.forEach((t, q) -> notifySubscriptionState(t, SubscriptionState.SUBSCRIBED, q.toInteger()));
        }

    }

    /**
     * Gets called when the disconnect call was processed.
     *
     * @param result The outcome of the disconnect call.
     */
    private void disconnectCompleted(AsyncResult<?> result) {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Disconnect completed - result: %s, cause: %s", result.result(), result.cause()));
        }

        connectionClosed(result.cause());
    }

    /**
     * Gets called internally when the only reasonable action is to just disconnect.
     * <p>
     * If the session is still running, then it will trigger a re-connect.
     *
     * @param reason The reason message.
     */
    private void closeConnection(final String reason) {
        closeConnection(new VertxException(reason).fillInStackTrace());
    }

    /**
     * Gets called when the connection just dropped.
     */
    private void connectionClosed() {
        if (this.state != SessionState.DISCONNECTING) {
            // this came unexpected
            connectionClosed(new VertxException("Connection closed"));
        }
    }

    /**
     * Called to clean up the after a connection was closed.
     *
     * @param cause The cause of the connection closure.
     */
    private void connectionClosed(final Throwable cause) {
        if (log.isDebugEnabled()) {
            log.debug("Connection closed", cause);
        } else {
            log.info("Connection closed: " + (cause != null ? cause.getMessage() : "<unknown>"));
        }

        if (this.client != null) {
            this.client.exceptionHandler(null);
            this.client.publishHandler(null);
            this.client.closeHandler(null);
            this.client.subscribeCompletionHandler(null);
            this.client.publishCompletionHandler(null);
            this.client.publishCompletionExpirationHandler(null);
            this.client.publishCompletionUnknownPacketIdHandler(null);
            this.client = null;
        }
        setState(SessionState.DISCONNECTED, cause);
    }

    /**
     * Gets called when the server published a message for us.
     *
     * @param message The published message.
     */
    private void serverPublished(MqttPublishMessage message) {
        if (log.isDebugEnabled()) {
            log.debug("Server published: " + message);
        }

        Handler<MqttPublishMessage> publishHandler = this.messageHandler;
        if (publishHandler != null) {
            publishHandler.handle(message);
        }
    }

    /**
     * Perform subscribing.
     *
     * @param topic The topics to subscribe to.
     */
    private void doSubscribe(String topic, RequestedQoS qos, Handler<AsyncResult<Integer>> handler) {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Request to subscribe to: %s / %s", topic, qos));
        }

        RequestedQoS current = this.subscriptions.get(topic);
        if (current != null) {
            if (log.isDebugEnabled()) {
                log.debug("Already subscribed with: " + current);
            }
            if (handler != null) {
                handler.handle(Future.succeededFuture(current.toInteger()));
            }
            return;
        }

        this.subscriptions.put(topic, qos);

        if (handler != null) {
            this.notifySubscribed.computeIfAbsent(topic, x -> new LinkedList<>())
                    .add(handler);
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("Requesting subscribe: %s / %s", topic, qos));
        }
        requestSubscribe(new LinkedHashMap<>(Collections.singletonMap(topic, qos)));
    }

    /**
     * Perform unsubscribing.
     *
     * @param topic The topics to unsubscribe from.
     */
    private void doUnsubscribe(String topic, Handler<AsyncResult<Void>> handler) {
        if (this.subscriptions.remove(topic) == null) {
            handler.handle(Future.succeededFuture());
            return;
        }

        if (handler != null) {
            this.notifyUnsubscribed.computeIfAbsent(topic, x -> new LinkedList<>())
                    .add(handler);
        }

        if (log.isDebugEnabled()) {
            log.debug("Requesting unsubscribe: " + topic);
        }

        requestUnsubscribe(Collections.singletonList(topic));
    }

    /**
     * Request to subscribe from the server.
     *
     * @param topics The topics to subscribe to, including the requested QoS.
     */
    private void requestSubscribe(LinkedHashMap<String, RequestedQoS> topics) {
        if (topics.isEmpty() || this.client == null || !this.client.isConnected()) {
            // nothing to do
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Request Subscribe to: " + topics);
        }

        this.client
                .subscribe(topics.entrySet()
                        .stream().collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().toInteger())))
                .onComplete(result -> subscribeSent(result, topics));
    }

    /**
     * Request to unsubscribe from the server.
     *
     * @param topics The topic to unsubscribe from.
     */
    private void requestUnsubscribe(List<String> topics) {
        if (topics.isEmpty() || this.client == null || !this.client.isConnected()) {
            // nothing to do
            return;
        }

        for (String topic : topics) {
            // vertx-mqtt currently does not support unsubscribing from multi-topics due to an API limitation
            this.client
                    .unsubscribe(topic)
                    .onComplete(result -> unsubscribeSent(result, Collections.singletonList(topic)));
        }
    }

    /**
     * Called when the subscribe call was sent.
     *
     * @param result The result of sending the request, contains the packet id.
     */
    private void subscribeSent(AsyncResult<Integer> result, LinkedHashMap<String, RequestedQoS> topics) {
        if (result.failed() || result.result() == null) {
            // failed
            for (String topic : topics.keySet()) {
                notifySubscriptionState(topic, SubscriptionState.UNSUBSCRIBED, null);
            }
        } else {
            // record request
            for (String topic : topics.keySet()) {
                notifySubscriptionState(topic, SubscriptionState.SUBSCRIBING, null);
            }
            this.pendingSubscribes.put(result.result(), topics);
        }
    }

    /**
     * Called when the unsubscribe call was sent.
     *
     * @param result The result of sending the request, contains the packet id.
     */
    private void unsubscribeSent(AsyncResult<Integer> result, List<String> topics) {
        if (result.failed() || result.result() == null) {
            closeConnection(String.format("Failed to send unsubscribe request: %s", result.cause()));
        } else {
            this.pendingUnsubscribes.put(result.result(), topics);
        }
    }

    /**
     * Called when the server processed the request to subscribe.
     *
     * @param ack The acknowledge message.
     */
    private void subscribeCompleted(MqttSubAckMessage ack) {
        LinkedHashMap<String, RequestedQoS> request = this.pendingSubscribes.remove(ack.messageId());
        if (request == null) {
            closeConnection(String.format("Unexpected subscription ack response - messageId: %s", ack.messageId()));
            return;
        }
        if (request.size() != ack.grantedQoSLevels().size()) {
            closeConnection(String.format("Mismatch of topics on subscription ack - expected: %d, actual: %d", request.size(),
                    ack.grantedQoSLevels().size()));
            return;
        }

        int idx = 0;
        for (String topic : request.keySet()) {
            Integer grantedQoS = ack.grantedQoSLevels().get(idx);
            notifySubscriptionState(topic, SubscriptionState.SUBSCRIBED, grantedQoS);
            idx += 1;
        }
    }

    /**
     * Called when the server processed the request to unsubscribe.
     *
     * @param messageId The ID of the message that completed.
     */
    private void unsubscribeCompleted(Integer messageId) {
        List<String> request = this.pendingUnsubscribes.remove(messageId);
        if (request != null) {
            for (String topic : request) {
                notifySubscriptionState(topic, SubscriptionState.UNSUBSCRIBED, null);
            }
        }
    }

    @Override
    public Future<Integer> publish(String topic, Buffer payload, MqttQoS qosLevel, boolean isDup, boolean isRetain) {
        Promise<Integer> future = Promise.promise();
        this.vertx
                .runOnContext(x -> doPublish(topic, payload, qosLevel, isDup, isRetain)
                        .onComplete(future));
        return future.future();
    }

    private Future<Integer> doPublish(String topic, Buffer payload, MqttQoS qosLevel, boolean isDup, boolean isRetain) {
        if (this.client != null && this.client.isConnected()) {
            // not checking for isConnected might throw a NPE from inside the client
            return this.client.publish(topic, payload, qosLevel, isDup, isRetain);
        } else {
            return Future.failedFuture("Session is not connected");
        }
    }

    private void publishComplete(Integer messageId) {
        Handler<Integer> handler = this.publishCompleteHandler;
        if (handler != null) {
            handler.handle(messageId);
        }
    }

    private void publishExpired(Integer messageId) {
        Handler<Integer> handler = this.publishCompletionExpirationHandler;
        if (handler != null) {
            handler.handle(messageId);
        }
    }

    private void publishCompletionUnknown(Integer messageId) {
        Handler<Integer> handler = this.publishCompletionUnknownPacketIdHandler;
        if (handler != null) {
            handler.handle(messageId);
        }
    }

}
