package io.smallrye.reactive.messaging.mqtt.session.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;

import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSession;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.mqtt.session.ReconnectDelayProvider;
import io.smallrye.reactive.messaging.mqtt.session.RequestedQoS;
import io.smallrye.reactive.messaging.mqtt.session.SessionEvent;
import io.smallrye.reactive.messaging.mqtt.session.SessionState;
import io.smallrye.reactive.messaging.mqtt.session.SubscriptionEvent;
import io.smallrye.reactive.messaging.mqtt.session.SubscriptionOptions;
import io.smallrye.reactive.messaging.mqtt.session.SubscriptionState;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.internal.VertxInternal;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttException;
import io.vertx.mqtt.messages.MqttAuthenticationExchangeMessage;
import io.vertx.mqtt.messages.MqttConnAckMessage;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;
import io.vertx.mqtt.messages.codes.MqttAuthenticateReasonCode;

public class MqttClientSessionImpl implements MqttClientSession {

    private static final Logger log = Logger.getLogger(MqttClientSessionImpl.class);

    private final VertxInternal vertx;
    private final MqttClientSessionOptions options;

    // record the subscriptions
    private final Map<String, SubscriptionOptions> subscriptions = new HashMap<>();
    // record the pending subscribes
    private final Map<Integer, LinkedHashMap<String, SubscriptionOptions>> pendingSubscribes = new HashMap<>();
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
    // stores the last CONNACK message for v5 properties
    private volatile MqttConnAckMessage connAckMessage;
    // MQTT v5: whether the server supports retained messages (null means supported, per spec default)
    private volatile Boolean retainAvailable;
    // MQTT v5: session expiry interval to send on DISCONNECT (null = use CONNECT value)
    private volatile Long disconnectSessionExpiryInterval;

    private volatile Handler<MqttPublishMessage> messageHandler;
    private volatile Handler<Throwable> exceptionHandler;
    private volatile Handler<SessionEvent> sessionStateHandler;
    private volatile Handler<SubscriptionEvent> subscriptionStateHandler;
    private volatile Handler<Integer> publishCompleteHandler;
    private volatile Handler<Integer> publishCompletionExpirationHandler;
    private volatile Handler<Integer> publishCompletionUnknownPacketIdHandler;
    private volatile Handler<MqttAuthenticationExchangeMessage> authenticationExchangeHandler;

    // tracks pending QoS 1/2 publishes: packetId → message + promise (for retry on reconnect)
    private final Map<Integer, PendingPublish> pendingPublishes = new HashMap<>();

    private final List<Promise<Void>> notifyConnected = new LinkedList<>();
    private final List<Promise<Void>> notifyStopped = new LinkedList<>();
    private final Map<String, List<Promise<Integer>>> notifySubscribed = new HashMap<>();
    private final Map<String, List<Promise<Void>>> notifyUnsubscribed = new HashMap<>();

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
    public MqttConnAckMessage getConnAckMessage() {
        return this.connAckMessage;
    }

    @Override
    public Future<Void> start() {
        Promise<Void> promise = Promise.promise();
        this.vertx.runOnContext(x -> doStart(promise));
        return promise.future();
    }

    @Override
    public Future<Void> stop() {
        return stop(null);
    }

    @Override
    public Future<Void> stop(Long sessionExpiryInterval) {
        this.disconnectSessionExpiryInterval = sessionExpiryInterval;
        Promise<Void> promise = Promise.promise();
        try {
            this.vertx.runOnContext(x -> doStop(promise));
        } catch (RejectedExecutionException e) {
            // Vert.x has been shutdown, ignore it.
        }
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
    public Future<Integer> subscribe(String topic, RequestedQoS qos, boolean noLocal, boolean retainAsPublished,
            int retainHandling) {
        return subscribe(topic, qos, noLocal, retainAsPublished, retainHandling, null);
    }

    @Override
    public Future<Integer> subscribe(String topic, RequestedQoS qos, boolean noLocal, boolean retainAsPublished,
            int retainHandling, Integer subscriptionIdentifier) {
        Promise<Integer> result = Promise.promise();
        SubscriptionOptions opts = new SubscriptionOptions(qos, noLocal, retainAsPublished, retainHandling,
                subscriptionIdentifier);
        this.vertx.runOnContext(x -> doSubscribe(topic, opts, result));
        return result.future();
    }

    @Override
    public Future<Void> unsubscribe(String topic) {
        Promise<Void> result = Promise.promise();
        this.vertx.runOnContext(x -> doUnsubscribe(topic, result));
        return result.future();
    }

    private void doStart(Promise<Void> handler) {
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

    private void doStop(Promise<Void> handler) {
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
    public MqttClientSession authenticationExchangeHandler(Handler<MqttAuthenticationExchangeMessage> handler) {
        this.authenticationExchangeHandler = handler;
        return this;
    }

    @Override
    public Future<Void> authenticate(MqttAuthenticateReasonCode reasonCode, MqttProperties properties) {
        if (this.client != null) {
            return this.client.authenticationExchange(reasonCode, properties);
        }
        return Future.failedFuture("Session is not connected");
    }

    @Override
    public MqttClientSession messageHandler(Handler<MqttPublishMessage> messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    private void setState(final SessionState sessionState, final Throwable cause) {
        setState(sessionState, cause, null);
    }

    /**
     * Set the state of the session.
     *
     * @param sessionState The new state.
     * @param cause The optional cause, in case of an error.
     * @param reasonCode The optional MQTT reason code (CONNACK or DISCONNECT).
     */
    private void setState(final SessionState sessionState, final Throwable cause, final Integer reasonCode) {

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
                if (!this.running || this.options.isCleanSession()) {
                    // Session is stopping or uses clean session: in-flight QoS 1/2 messages
                    // are lost, so we fail the pending publish promises.
                    for (PendingPublish pending : this.pendingPublishes.values()) {
                        pending.completion.fail("Session disconnected");
                    }
                    this.pendingPublishes.clear();
                }
                // Persistent session (running && !cleanSession): pending publishes are
                // kept and will be resent with isDup=true on reconnect.
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
                handler.handle(new SessionEventImpl(sessionState, cause, reasonCode));
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
                    for (Promise<Void> handler : this.notifyConnected) {
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
                    for (Promise<Void> handler : this.notifyConnected) {
                        handler.handle(Future.failedFuture("Session stopped"));
                    }
                    this.notifyConnected.clear();
                    for (Promise<Void> handler : this.notifyStopped) {
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
            List<Promise<Integer>> handlers = this.notifySubscribed.remove(topic);
            if (handlers != null) {
                for (Promise<Integer> handler : handlers) {
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
            List<Promise<Void>> handlers = this.notifyUnsubscribed.remove(topic);
            if (handlers != null) {
                for (Promise<Void> handler : handlers) {
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

        // MQTT v5 handlers
        Handler<MqttAuthenticationExchangeMessage> authHandler = this.authenticationExchangeHandler;
        if (authHandler != null) {
            this.client.authenticationExchangeHandler(authHandler);
        }
        this.client.disconnectMessageHandler(msg -> {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Server-initiated DISCONNECT - reason code: %s", msg.code()));
            }
            Throwable cause = new VertxException("Server sent DISCONNECT: " + msg.code());
            Integer reasonCode = (int) msg.code().value() & 0xFF;
            setState(SessionState.DISCONNECTING, cause, reasonCode);
            this.client.disconnect().onComplete(this::disconnectCompleted);
        });
        this.client.publishAckMessageHandler(ack -> {
            if (ack.code() != null && ack.code().isError()) {
                log.warnf("PUBACK error reason code: %s (packet %d)", ack.code(), ack.messageId());
                PendingPublish pending = this.pendingPublishes.remove(ack.messageId());
                if (pending != null) {
                    pending.completion.fail(new VertxException("PUBACK error: " + ack.code()));
                }
            }
        });

        // change state
        setState(SessionState.CONNECTING, null);
        // start connection
        this.client
                .connect(
                        this.options.getPort(),
                        this.options.getHostname(),
                        this.options.getServerName().orElse(this.options.getHostname()),
                        this.options.getConnectUserProperties())
                .onComplete(this::connectCompleted);
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

        Long sessionExpiry = this.disconnectSessionExpiryInterval;
        if (sessionExpiry != null) {
            // MQTT v5: send session expiry override on DISCONNECT
            MqttProperties disconnectProps = new MqttProperties();
            disconnectProps.add(new MqttProperties.IntegerProperty(
                    MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), sessionExpiry.intValue()));
            this.client.disconnect(
                    io.vertx.mqtt.messages.codes.MqttDisconnectReasonCode.NORMAL,
                    disconnectProps).onComplete(this::disconnectCompleted);
        } else {
            this.client.disconnect().onComplete(this::disconnectCompleted);
        }
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
            Integer code = result.result() != null ? (int) result.result().code().byteValue() : null;
            setState(SessionState.DISCONNECTED, result.cause(), code);
            return;
        }

        MqttConnAckMessage ack = result.result();
        this.connAckMessage = ack;
        this.retainAvailable = ack.retainAvailable();

        if (log.isDebugEnabled()) {
            // Log v5 CONNACK properties when available
            if (ack.assignedClientIdentifier() != null) {
                log.debug("Server assigned client ID: " + ack.assignedClientIdentifier());
            }
            if (ack.serverKeepAlive() != null) {
                log.debug("Server keep alive: " + ack.serverKeepAlive());
            }
            if (ack.maximumQos() != null) {
                log.debug("Server max QoS: " + ack.maximumQos());
            }
            if (ack.sessionExpiryInterval() != null) {
                log.debug("Server session expiry interval: " + ack.sessionExpiryInterval());
            }
            if (ack.topicAliasMaximum() != null) {
                log.debug("Server topic alias maximum: " + ack.topicAliasMaximum());
            }
            if (ack.reasonString() != null) {
                log.debug("CONNACK reason: " + ack.reasonString());
            }
        }

        setState(SessionState.CONNECTED, null, (int) ack.code().byteValue());

        if (log.isDebugEnabled()) {
            log.debug(String.format("Subscriptions: %s, cleanSession: %s, sessionPresent: %s", !this.subscriptions.isEmpty(),
                    options.isCleanSession(), ack.isSessionPresent()));
        }

        // Resend unacked QoS 1/2 messages from a previous connection with isDup=true
        if (!options.isCleanSession() && ack.isSessionPresent()) {
            resendPendingPublishes();
        }

        if (options.isCleanSession() || !ack.isSessionPresent()) {
            // re-subscribe if we have requested subscriptions and (either cleanSession=true or no session found on the server)
            requestSubscribe(new LinkedHashMap<>(this.subscriptions));
        } else {
            // If the session is present on broker, I mark all subscription to SUBSC
            log.debug("Session present on broker, subscriptions request not sent. "
                    + "Be sure that the subscriptions on the broker side are the same that this client needs.");
            this.subscriptions.forEach(
                    (t, opts) -> notifySubscriptionState(t, SubscriptionState.SUBSCRIBED, opts.getQos().toInteger()));
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
    private void doSubscribe(String topic, RequestedQoS qos, Promise<Integer> handler) {
        doSubscribe(topic, new SubscriptionOptions(qos), handler);
    }

    private void doSubscribe(String topic, SubscriptionOptions options, Promise<Integer> handler) {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Request to subscribe to: %s / %s", topic, options.getQos()));
        }

        SubscriptionOptions current = this.subscriptions.get(topic);
        if (current != null) {
            if (log.isDebugEnabled()) {
                log.debug("Already subscribed with: " + current.getQos());
            }
            if (handler != null) {
                handler.handle(Future.succeededFuture(current.getQos().toInteger()));
            }
            return;
        }

        this.subscriptions.put(topic, options);

        if (handler != null) {
            this.notifySubscribed.computeIfAbsent(topic, x -> new LinkedList<>())
                    .add(handler);
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("Requesting subscribe: %s / %s", topic, options.getQos()));
        }
        requestSubscribe(new LinkedHashMap<>(Collections.singletonMap(topic, options)));
    }

    /**
     * Perform unsubscribing.
     *
     * @param topic The topics to unsubscribe from.
     */
    private void doUnsubscribe(String topic, Promise<Void> handler) {
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
     * @param topics The topics to subscribe to, including the subscription options.
     */
    private void requestSubscribe(LinkedHashMap<String, SubscriptionOptions> topics) {
        if (topics.isEmpty() || this.client == null || !this.client.isConnected()) {
            // nothing to do
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Request Subscribe to: " + topics);
        }

        boolean hasV5Options = topics.values().stream().anyMatch(SubscriptionOptions::hasV5Options);

        if (hasV5Options) {
            // Use MQTT v5 subscription with options
            List<MqttTopicSubscription> subscriptionList = new ArrayList<>();
            for (Map.Entry<String, SubscriptionOptions> entry : topics.entrySet()) {
                SubscriptionOptions opts = entry.getValue();
                MqttSubscriptionOption option = MqttSubscriptionOption.onlyFromQos(
                        MqttQoS.valueOf(opts.getQos().toInteger()));
                if (opts.hasV5Options()) {
                    option = new MqttSubscriptionOption(
                            MqttQoS.valueOf(opts.getQos().toInteger()),
                            opts.isNoLocal(),
                            opts.isRetainAsPublished(),
                            MqttSubscriptionOption.RetainedHandlingPolicy.valueOf(opts.getRetainHandling()));
                }
                subscriptionList.add(new MqttTopicSubscription(entry.getKey(), option));
            }
            // Build MQTT properties with subscription identifier if present
            MqttProperties subscribeProps = MqttProperties.NO_PROPERTIES;
            Integer subId = topics.values().stream()
                    .map(SubscriptionOptions::getSubscriptionIdentifier)
                    .filter(id -> id != null)
                    .findFirst().orElse(null);
            if (subId != null) {
                subscribeProps = new MqttProperties();
                subscribeProps.add(new MqttProperties.IntegerProperty(
                        MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER.value(), subId));
            }

            this.client
                    .subscribe(subscriptionList, subscribeProps)
                    .onComplete(result -> subscribeSent(result, topics));
        } else {
            // Use standard MQTT 3.1.1 subscribe
            this.client
                    .subscribe(topics.entrySet()
                            .stream().collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> e.getValue().getQos().toInteger())))
                    .onComplete(result -> subscribeSent(result, topics));
        }
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
    private void subscribeSent(AsyncResult<Integer> result, LinkedHashMap<String, SubscriptionOptions> topics) {
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
        LinkedHashMap<String, SubscriptionOptions> request = this.pendingSubscribes.remove(ack.messageId());
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
            if (grantedQoS != null && grantedQoS >= 0x80) {
                // MQTT v5 SUBACK failure reason code (>= 0x80 indicates an error)
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Subscription failed for topic %s with reason code 0x%02X", topic, grantedQoS));
                }
                notifySubscriptionState(topic, SubscriptionState.FAILED, grantedQoS);
            } else {
                notifySubscriptionState(topic, SubscriptionState.SUBSCRIBED, grantedQoS);
            }
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
        return doPublish(topic, payload, qosLevel, isDup, isRetain, null);
    }

    @Override
    public Future<Integer> publish(String topic, Buffer payload, MqttQoS qosLevel, boolean isDup, boolean isRetain,
            MqttProperties properties) {
        Promise<Integer> future = Promise.promise();
        this.vertx
                .runOnContext(x -> doPublish(topic, payload, qosLevel, isDup, isRetain, properties)
                        .onComplete(future));
        return future.future();
    }

    private Future<Integer> doPublish(String topic, Buffer payload, MqttQoS qosLevel, boolean isDup, boolean isRetain,
            MqttProperties properties) {
        if (this.client == null || !this.client.isConnected()) {
            return Future.failedFuture("Session is not connected");
        }

        // MQTT v5: reject retain if the server does not support it
        if (isRetain && Boolean.FALSE.equals(this.retainAvailable)) {
            return Future.failedFuture("Server does not support retained messages");
        }

        Future<Integer> sendFuture;
        if (properties != null) {
            sendFuture = this.client.publish(topic, payload, qosLevel, isDup, isRetain, properties);
        } else {
            sendFuture = this.client.publish(topic, payload, qosLevel, isDup, isRetain);
        }

        if (qosLevel == MqttQoS.AT_MOST_ONCE) {
            // QoS 0: fire-and-forget, complete when packet is sent
            return sendFuture;
        }

        // QoS 1/2: complete when PUBACK (QoS 1) or PUBCOMP (QoS 2) is received
        Promise<Integer> completion = Promise.promise();
        PendingPublish pending = new PendingPublish(-1, topic, payload, qosLevel, isRetain, properties, completion);
        sendFuture.onSuccess(packetId -> {
            pending.setMessageId(packetId);
            this.pendingPublishes.put(packetId, pending);
        }).onFailure(err -> {
            if (isRetryableSendError(err)) {
                retryOrFail(pending, err);
            } else {
                completion.fail(err);
            }
        });
        return completion.future();
    }

    private void publishComplete(Integer messageId) {
        PendingPublish pending = this.pendingPublishes.remove(messageId);
        if (pending != null) {
            pending.completion.complete(messageId);
        }
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
        PendingPublish pending = this.pendingPublishes.get(messageId);
        if (pending != null) {
            retryOrFail(pending, new VertxException("Publish completion expired (packetId=" + messageId + ")"));
        }
    }

    private void publishCompletionUnknown(Integer messageId) {
        Handler<Integer> handler = this.publishCompletionUnknownPacketIdHandler;
        if (handler != null) {
            handler.handle(messageId);
        }
    }

    private void resendPendingPublishes() {
        if (this.pendingPublishes.isEmpty()) {
            return;
        }

        log.debugf("Resending %d pending QoS 1/2 publishes after reconnect", this.pendingPublishes.size());

        for (PendingPublish pending : new ArrayList<>(this.pendingPublishes.values())) {
            Future<Integer> sendFuture;
            if (pending.properties != null) {
                sendFuture = this.client.publish(pending.messageId, pending.topic, pending.payload,
                        pending.qosLevel, true, pending.isRetain /* TODO , pending.properties */ );
            } else {
                sendFuture = this.client.publish(pending.messageId, pending.topic, pending.payload,
                        pending.qosLevel, true, pending.isRetain);
            }
            sendFuture.onFailure(err -> {
                log.warnf("Failed to resend pending publish (packetId=%d): %s", pending.messageId, err.getMessage());
                PendingPublish removed = this.pendingPublishes.remove(pending.messageId);
                if (removed != null) {
                    removed.completion.fail(err);
                }
            });
        }
    }

    private static boolean isRetryableSendError(Throwable err) {
        return err instanceof MqttException && ((MqttException) err).code() == MqttException.MQTT_INFLIGHT_QUEUE_FULL;
    }

    private void retryOrFail(PendingPublish pending, Throwable cause) {
        int maxRetries = this.options.getPublishMaxRetries();
        if (pending.retryCount >= maxRetries || !this.running) {
            log.warnf("Publish failed after %d retries: %s", pending.retryCount, cause.getMessage());
            this.pendingPublishes.remove(pending.messageId);
            pending.completion.fail(cause);
            return;
        }

        pending.retryCount++;
        long delay = 100L * pending.retryCount;
        log.debugf("Scheduling publish retry %d/%d in %dms (packetId=%d): %s",
                pending.retryCount, maxRetries, delay, pending.messageId, cause.getMessage());

        this.vertx.setTimer(delay, timerId -> {
            if (!this.running || this.client == null || !this.client.isConnected()) {
                return;
            }

            Future<Integer> sendFuture;
            if (pending.messageId >= 0) {
                // Retry with same message ID and isDup=true
                if (pending.properties != null) {
                    sendFuture = this.client.publish(pending.messageId, pending.topic, pending.payload,
                            pending.qosLevel, true, pending.isRetain/* TODO, pending.properties */);
                } else {
                    sendFuture = this.client.publish(pending.messageId, pending.topic, pending.payload,
                            pending.qosLevel, true, pending.isRetain);
                }
            } else {
                // Never sent successfully — use a fresh message ID
                if (pending.properties != null) {
                    sendFuture = this.client.publish(pending.topic, pending.payload,
                            pending.qosLevel, false, pending.isRetain, pending.properties);
                } else {
                    sendFuture = this.client.publish(pending.topic, pending.payload,
                            pending.qosLevel, false, pending.isRetain);
                }
            }
            sendFuture.onSuccess(packetId -> {
                pending.setMessageId(packetId);
                this.pendingPublishes.put(packetId, pending);
            }).onFailure(err -> retryOrFail(pending, err));
        });
    }

    static final class PendingPublish {
        private int messageId;
        final String topic;
        final Buffer payload;
        final MqttQoS qosLevel;
        final boolean isRetain;
        final MqttProperties properties;
        final Promise<Integer> completion;
        int retryCount;

        PendingPublish(int messageId, String topic, Buffer payload, MqttQoS qosLevel,
                boolean isRetain, MqttProperties properties, Promise<Integer> completion) {
            this.messageId = messageId;
            this.topic = topic;
            this.payload = payload;
            this.qosLevel = qosLevel;
            this.isRetain = isRetain;
            this.properties = properties;
            this.completion = completion;
        }

        void setMessageId(int messageId) {
            this.messageId = messageId;
        }
    }

}
