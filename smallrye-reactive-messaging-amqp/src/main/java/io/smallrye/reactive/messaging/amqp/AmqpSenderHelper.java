package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.AmqpConnector.PAIRED_KEY;
import static io.smallrye.reactive.messaging.amqp.ChannelUtils.getClientCapabilities;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Target;

import io.smallrye.mutiny.Uni;
import io.vertx.amqp.impl.AmqpConnectionImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.mutiny.amqp.AmqpConnection;
import io.vertx.mutiny.amqp.AmqpSender;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonLinkOptions;
import io.vertx.proton.ProtonSender;

public class AmqpSenderHelper {

    private AmqpSenderHelper() {
        // avoid direct instantiation.
    }

    public static Uni<AmqpSender> createSenderWithProperties(ConnectionHolder holder, AmqpConnection connection,
            String address, String linkName, AmqpConnectorOutgoingConfiguration oc, boolean linkPairing) {

        return Uni.createFrom().emitter(emitter -> {
            holder.runWithTrampoline(() -> {
                try {
                    ProtonConnection protonConn = connection.getDelegate().unwrap();
                    ProtonLinkOptions linkOpts = new ProtonLinkOptions()
                            .setLinkName(linkName);
                    ProtonSender protonSender = protonConn.createSender(address, linkOpts);
                    protonSender.setAutoDrained(true);

                    List<String> capabilities = getClientCapabilities(oc);
                    if (!capabilities.isEmpty()) {
                        Target target = (Target) protonSender.getTarget();
                        target.setCapabilities(capabilities.stream()
                                .map(Symbol::valueOf).toArray(Symbol[]::new));
                    }
                    if (linkPairing) {
                        Map<Symbol, Object> linkProperties = new HashMap<>();
                        linkProperties.put(PAIRED_KEY, true);
                        protonSender.setProperties(linkProperties);
                    }

                    Constructor<?> ctor = Class.forName("io.vertx.amqp.impl.AmqpSenderImpl")
                            .getDeclaredConstructor(
                                    ProtonSender.class,
                                    AmqpConnectionImpl.class,
                                    Handler.class);
                    ctor.setAccessible(true);

                    Handler<AsyncResult<io.vertx.amqp.AmqpSender>> handler = ar -> {
                        if (ar.succeeded()) {
                            emitter.complete(AmqpSender.newInstance(ar.result()));
                        } else {
                            emitter.fail(ar.cause());
                        }
                    };
                    ctor.newInstance(protonSender, connection.getDelegate(), handler);
                } catch (Exception e) {
                    emitter.fail(e);
                }
            });
        });
    }
}
