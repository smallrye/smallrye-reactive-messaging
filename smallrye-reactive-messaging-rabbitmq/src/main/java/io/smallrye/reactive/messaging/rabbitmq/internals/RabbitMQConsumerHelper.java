package io.smallrye.reactive.messaging.rabbitmq.internals;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;
import static io.smallrye.reactive.messaging.rabbitmq.internals.RabbitMQClientHelper.parseArguments;
import static io.smallrye.reactive.messaging.rabbitmq.internals.RabbitMQClientHelper.serverQueueName;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorIncomingConfiguration;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;

public class RabbitMQConsumerHelper {

    /**
     * Uses a {@link RabbitMQClient} to ensure the required exchange is created.
     *
     * @param client the RabbitMQ client
     * @param config the channel configuration
     * @return a {@link Uni <String>} which yields the exchange name
     */
    static Uni<String> declareExchangeIfNeeded(
            final RabbitMQClient client,
            final RabbitMQConnectorCommonConfiguration config,
            final Instance<Map<String, ?>> configMaps) {
        final String exchangeName = getExchangeName(config);

        JsonObject queueArgs = new JsonObject();
        Instance<Map<String, ?>> queueArguments = CDIUtils.getInstanceById(configMaps, config.getExchangeArguments());
        if (queueArguments.isResolvable()) {
            Map<String, ?> argsMap = queueArguments.get();
            argsMap.forEach(queueArgs::put);
        }

        // Declare the exchange if we have been asked to do so and only when exchange name is not default ("")
        boolean declareExchange = config.getExchangeDeclare() && !exchangeName.isEmpty();
        if (declareExchange) {
            return client
                    .exchangeDeclare(exchangeName, config.getExchangeType(),
                            config.getExchangeDurable(), config.getExchangeAutoDelete(), queueArgs)
                    .replaceWith(exchangeName)
                    .invoke(() -> log.exchangeEstablished(exchangeName))
                    .onFailure().invoke(ex -> log.unableToEstablishExchange(exchangeName, ex));
        } else {
            return Uni.createFrom().item(exchangeName);
        }
    }

    public static String getExchangeName(final RabbitMQConnectorCommonConfiguration config) {
        return config.getExchangeName().map(s -> "\"\"".equals(s) ? "" : s).orElse(config.getChannel());
    }

    /**
     * Establish a DLQ, possibly establishing a DLX too
     *
     * @param client the {@link RabbitMQClient}
     * @param ic the {@link RabbitMQConnectorIncomingConfiguration}
     * @return a {@link Uni<String>} containing the DLQ name
     */
    static Uni<?> configureDLQorDLX(final RabbitMQClient client, final RabbitMQConnectorIncomingConfiguration ic,
            final Instance<Map<String, ?>> configMaps) {
        final String deadLetterQueueName = ic.getDeadLetterQueueName().orElse(String.format("%s.dlq", getQueueName(ic)));
        final String deadLetterExchangeName = ic.getDeadLetterExchange();
        final String deadLetterRoutingKey = ic.getDeadLetterRoutingKey().orElse(getQueueName(ic));

        final JsonObject exchangeArgs = new JsonObject();
        ic.getDeadLetterExchangeArguments().ifPresent(argsId -> {
            Instance<Map<String, ?>> exchangeArguments = CDIUtils.getInstanceById(configMaps, argsId);
            if (exchangeArguments.isResolvable()) {
                Map<String, ?> argsMap = exchangeArguments.get();
                argsMap.forEach(exchangeArgs::put);
            }
        });
        // Declare the exchange if we have been asked to do so
        final Uni<String> dlxFlow = Uni.createFrom()
                .item(() -> ic.getAutoBindDlq() && ic.getDlxDeclare() ? null : deadLetterExchangeName)
                .onItem().ifNull().switchTo(() -> client.exchangeDeclare(deadLetterExchangeName, ic.getDeadLetterExchangeType(),
                        true, false, exchangeArgs)
                        .onItem().invoke(() -> log.dlxEstablished(deadLetterExchangeName))
                        .onFailure().invoke(ex -> log.unableToEstablishDlx(deadLetterExchangeName, ex))
                        .onItem().transform(v -> deadLetterExchangeName));

        // Declare the queue (and its binding to the exchange or DLQ type/mode) if we have been asked to do so
        final JsonObject queueArgs = new JsonObject();
        ic.getDeadLetterQueueArguments().ifPresent(argsId -> {
            Instance<Map<String, ?>> queueArguments = CDIUtils.getInstanceById(configMaps, argsId);
            if (queueArguments.isResolvable()) {
                Map<String, ?> argsMap = queueArguments.get();
                argsMap.forEach(queueArgs::put);
            }
        });
        // x-dead-letter-exchange
        ic.getDeadLetterDlx().ifPresent(deadLetterDlx -> queueArgs.put("x-dead-letter-exchange", deadLetterDlx));
        // x-dead-letter-routing-key
        ic.getDeadLetterDlxRoutingKey().ifPresent(deadLetterDlx -> queueArgs.put("x-dead-letter-routing-key", deadLetterDlx));
        // x-queue-type
        ic.getDeadLetterQueueType().ifPresent(queueType -> queueArgs.put("x-queue-type", queueType));
        // x-queue-mode
        ic.getDeadLetterQueueMode().ifPresent(queueMode -> queueArgs.put("x-queue-mode", queueMode));
        // x-message-ttl
        ic.getDeadLetterTtl().ifPresent(queueTtl -> {
            if (queueTtl >= 0) {
                queueArgs.put("x-message-ttl", queueTtl);
            } else {
                throw ex.illegalArgumentInvalidQueueTtl();
            }
        });
        return dlxFlow
                .onItem().transform(v -> Boolean.TRUE.equals(ic.getAutoBindDlq()) ? null : deadLetterQueueName)
                .onItem().ifNull().switchTo(
                        () -> client
                                .queueDeclare(deadLetterQueueName, true, false, false, queueArgs)
                                .onItem().invoke(() -> log.queueEstablished(deadLetterQueueName))
                                .onFailure().invoke(ex -> log.unableToEstablishQueue(deadLetterQueueName, ex))
                                .onItem()
                                .call(v -> client.queueBind(deadLetterQueueName, deadLetterExchangeName, deadLetterRoutingKey))
                                .onItem()
                                .invoke(() -> log.deadLetterBindingEstablished(deadLetterQueueName, deadLetterExchangeName,
                                        deadLetterRoutingKey))
                                .onFailure()
                                .invoke(ex -> log.unableToEstablishBinding(deadLetterQueueName, deadLetterExchangeName, ex))
                                .onItem().transform(v -> deadLetterQueueName));
    }

    /**
     * Returns a stream that will create bindings from the queue to the exchange with each of the
     * supplied routing keys.
     *
     * @param client the {@link RabbitMQClient} to use
     * @param ic the incoming channel configuration
     * @return a Uni with the list of routing keys
     */
    static Uni<List<String>> establishBindings(
            final RabbitMQClient client,
            final RabbitMQConnectorIncomingConfiguration ic) {
        final String exchangeName = getExchangeName(ic);
        final String queueName = getQueueName(ic);
        final List<String> routingKeys = Arrays.stream(ic.getRoutingKeys().split(","))
                .map(String::trim).collect(Collectors.toList());
        final Map<String, Object> arguments = parseArguments(ic.getArguments());

        // Skip queue bindings if exchange name is default ("")
        if (exchangeName.isEmpty()) {
            return Uni.createFrom().item(Collections.emptyList());
        }

        return Multi.createFrom().iterable(routingKeys)
                .call(routingKey -> client.queueBind(serverQueueName(queueName), exchangeName, routingKey, arguments))
                .invoke(routingKey -> log.bindingEstablished(queueName, exchangeName, routingKey, arguments.toString()))
                .onFailure().invoke(ex -> log.unableToEstablishBinding(queueName, exchangeName, ex))
                .collect().asList();
    }

    public static String getQueueName(final RabbitMQConnectorIncomingConfiguration config) {
        return config.getQueueName().orElse(config.getChannel());
    }
}
