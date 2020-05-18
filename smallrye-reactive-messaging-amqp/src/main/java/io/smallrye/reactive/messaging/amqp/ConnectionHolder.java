package io.smallrye.reactive.messaging.amqp;

import static java.time.Duration.ofSeconds;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpConnection;

public class ConnectionHolder {

    private final AmqpClient client;
    private final AmqpConnectorCommonConfiguration configuration;
    private final AtomicReference<AmqpConnection> holder = new AtomicReference<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHolder.class);
    private Consumer<Throwable> callback;

    public ConnectionHolder(AmqpClient client,
            AmqpConnectorCommonConfiguration configuration) {
        this.client = client;
        this.configuration = configuration;
    }

    public synchronized void onFailure(Consumer<Throwable> callback) {
        this.callback = callback;
    }

    public Uni<AmqpConnection> getOrEstablishConnection() {
        return Uni.createFrom().item(holder::get)
                .onItem().ifNull().switchTo(() -> {
                    // we don't have a connection, try to connect.
                    Integer retryInterval = configuration.getReconnectInterval();
                    Integer retryAttempts = configuration.getReconnectAttempts();

                    AmqpConnection current = holder.get();
                    if (current != null && !current.getDelegate().isDisconnected()) {
                        return Uni.createFrom().item(current);
                    }

                    return client.connect()
                            .on().subscribed(s -> LOGGER.info("Establishing connection with AMQP broker"))
                            .onItem().apply(conn -> {
                                LOGGER.info("Connection with AMQP broker established");
                                holder.set(conn);
                                conn
                                        .exceptionHandler(t -> {
                                            holder.set(null);
                                            LOGGER.error("AMQP Connection failure", t);

                                            // The callback failure allows propagating the failure downstream,
                                            // as we are disconnected from the flow.
                                            Consumer<Throwable> c;
                                            synchronized (this) {
                                                c = callback;
                                            }
                                            if (c != null) {
                                                c.accept(t);
                                            }
                                        });
                                // handle the case we are already disconnected.
                                if (conn.getDelegate().isDisconnected() || holder.get() == null) {
                                    // Throwing the exception would trigger a retry.
                                    holder.set(null);
                                    throw new IllegalStateException("AMQP Connection disconnected");
                                }
                                return conn;
                            })
                            .onFailure()
                            .invoke(t -> LOGGER.error("Unable to connect to the broker, retry will be attempted", t))
                            .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(retryInterval)).atMost(retryAttempts)
                            .onFailure().invoke(t -> {
                                holder.set(null);
                                LOGGER.error("Unable to recover from AMQP connection disruption", t);
                            });
                });
    }
}
