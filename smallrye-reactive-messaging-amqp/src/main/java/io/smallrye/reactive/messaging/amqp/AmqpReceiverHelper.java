package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.AmqpConnector.DIRECT_REPLY_TO_ADDRESS;
import static io.smallrye.reactive.messaging.amqp.AmqpConnector.PAIRED_KEY;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnknownDescribedType;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;

import io.smallrye.mutiny.Uni;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.amqp.impl.AmqpConnectionImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.amqp.AmqpReceiver;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonLinkOptions;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;

public class AmqpReceiverHelper {

    // For the "apache.org:no-local-filter:list" filter
    private static final Symbol NO_LOCAL_KEY = Symbol.valueOf("no-local");
    private static final UnsignedLong NO_LOCAL_DESCRIPTOR = UnsignedLong.valueOf(0x0000_468C_0000_0003L);
    private static final DescribedType NO_LOCAL_FILTER = new UnknownDescribedType(NO_LOCAL_DESCRIPTOR, "NoLocalFilter{}");

    // For the "apache.org:selector-filter:string" filter
    private static final Symbol SELECTOR_KEY = Symbol.valueOf("selector");
    private static final UnsignedLong SELECTOR_DESCRIPTOR = UnsignedLong.valueOf(0x0000_468C_0000_0004L);

    private AmqpReceiverHelper() {
        // avoid direct instantiation.
    }

    public static Uni<AmqpReceiver> createReceiver(ConnectionHolder holder, AmqpConnection connection, String address,
            AmqpReceiverOptions options, boolean linkPairing) {
        boolean directReplyTo = DIRECT_REPLY_TO_ADDRESS.equals(address);
        if (!directReplyTo && !linkPairing) {
            return connection.createReceiver(address, options);
        }
        return Uni.createFrom().emitter(emitter -> {
            holder.runWithTrampoline(() -> {
                try {
                    io.vertx.amqp.AmqpConnection amqpConnection = connection.getDelegate();
                    ProtonConnection protonConn = amqpConnection.unwrap();
                    if (protonConn == null) {
                        emitter.fail(new NoStackTraceThrowable("Not connected"));
                        return;
                    }

                    String addressToListen = address;

                    if (directReplyTo) {
                        options.setDynamic(true);
                        List<String> caps = new ArrayList<>(options.getCapabilities());
                        caps.add("rabbitmq:volatile-queue");
                        options.setCapabilities(caps);
                        addressToListen = null;
                    }
                    ProtonLinkOptions linkOpts = new ProtonLinkOptions()
                            .setDynamic(options.isDynamic())
                            .setLinkName(options.getLinkName());
                    ProtonReceiver receiver = protonConn.createReceiver(addressToListen, linkOpts);

                    if (options.getQos() != null) {
                        receiver.setQoS(ProtonQoS.valueOf(options.getQos().toUpperCase()));
                    }
                    configureTheSource(options, receiver);
                    // set link properties
                    if (linkPairing) {
                        Map<Symbol, Object> linkProperties = new HashMap<>();
                        linkProperties.put(PAIRED_KEY, true);
                        receiver.setProperties(linkProperties);
                    }
                    if (directReplyTo) {
                        // force SenderSettleMode.SETTLED;
                        receiver.setQoS(ProtonQoS.AT_MOST_ONCE);
                        Source source = (Source) receiver.getSource();
                        source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
                        source.setDurable(TerminusDurability.NONE);
                        source.setTimeout(UnsignedInteger.ZERO);
                    }

                    Constructor<?> ctor = Class.forName("io.vertx.amqp.impl.AmqpReceiverImpl")
                            .getDeclaredConstructor(
                                    String.class,
                                    AmqpConnectionImpl.class,
                                    AmqpReceiverOptions.class,
                                    ProtonReceiver.class,
                                    Handler.class);
                    ctor.setAccessible(true);

                    Handler<AsyncResult<io.vertx.amqp.AmqpReceiver>> handler = ar -> {
                        if (ar.succeeded()) {
                            emitter.complete(AmqpReceiver.newInstance(ar.result()));
                        } else {
                            emitter.fail(ar.cause());
                        }
                    };
                    ctor.newInstance(addressToListen, amqpConnection, options, receiver, handler);
                } catch (Exception e) {
                    emitter.fail(e);
                }
            });
        });
    }

    private static void configureTheSource(AmqpReceiverOptions receiverOptions, ProtonReceiver receiver) {
        Source source = (Source) receiver.getSource();

        List<String> capabilities = receiverOptions.getCapabilities();
        if (!capabilities.isEmpty()) {
            source.setCapabilities(capabilities.stream().map(Symbol::valueOf).toArray(Symbol[]::new));
        }

        if (receiverOptions.isDurable()) {
            source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
            source.setDurable(TerminusDurability.UNSETTLED_STATE);
        }

        final Map<Symbol, DescribedType> filters = new HashMap<>();

        if (receiverOptions.isNoLocal()) {
            filters.put(NO_LOCAL_KEY, NO_LOCAL_FILTER);
        }

        final String selector = receiverOptions.getSelector();
        if (selector != null && !selector.trim().isEmpty()) {
            filters.put(SELECTOR_KEY, new UnknownDescribedType(SELECTOR_DESCRIPTOR, selector));
        }

        if (!filters.isEmpty()) {
            source.setFilter(filters);
        }
    }
}
