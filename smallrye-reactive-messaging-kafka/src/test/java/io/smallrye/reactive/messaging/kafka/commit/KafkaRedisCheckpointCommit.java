package io.smallrye.reactive.messaging.kafka.commit;

import static io.vertx.mutiny.redis.client.Request.cmd;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.impl.JsonHelper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.Response;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.RedisOptionsConverter;

public class KafkaRedisCheckpointCommit extends KafkaCheckpointCommit {

    public static final String REDIS_CHECKPOINT_NAME = "checkpoint-redis";

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Redis redis;

    public KafkaRedisCheckpointCommit(KafkaConnectorIncomingConfiguration config,
            Vertx vertx,
            KafkaConsumer<?, ?> consumer,
            BiConsumer<Throwable, Boolean> reportFailure,
            int defaultTimeout,
            Redis redis) {
        super(vertx, config, consumer, reportFailure, defaultTimeout);
        this.redis = redis;
    }

    private <T> Uni<T> runWithRedis(Function<Redis, Uni<T>> action) {
        return Uni.createFrom().deferred(() -> {
            if (started.compareAndSet(false, true)) {
                return redis.connect().replaceWith(redis)
                        .onFailure().invoke(t -> {
                            started.set(false);
                            reportFailure.accept(t, true);
                        });
            } else {
                return Uni.createFrom().item(redis);
            }
        })
                .chain(action::apply);
    }

    @ApplicationScoped
    @Identifier(REDIS_CHECKPOINT_NAME)
    public static class Factory implements KafkaCommitHandler.Factory {

        @Override
        public KafkaCommitHandler create(KafkaConnectorIncomingConfiguration config, Vertx vertx,
                KafkaConsumer<?, ?> consumer, BiConsumer<Throwable, Boolean> reportFailure) {
            int defaultTimeout = config.config()
                    .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class)
                    .orElse(60000);
            JsonObject entries = JsonHelper.asJsonObject(config.config(), REDIS_CHECKPOINT_NAME + ".");
            RedisOptions options = new RedisOptions();
            RedisOptionsConverter.fromJson(entries, options);
            Redis redis = Redis.createClient(vertx, options);

            return new KafkaRedisCheckpointCommit(config, vertx, consumer, reportFailure, defaultTimeout, redis);
        }

    }

    @Override
    public void terminate(boolean graceful) {
        super.terminate(graceful);
        redis.close();
        started.set(false);
    }

    @Override
    protected Uni<ProcessingState<?>> fetchProcessingState(TopicPartition partition) {
        return runWithRedis(redis -> redis.send(cmd(Command.GET).arg(getKey(partition)))
                .onFailure().invoke(t -> log.errorf(t, "Error fetching processing state %s", partition))
                .onItem().invoke(r -> log.debugf("Fetched state for partition %s : %s", partition, r))
                .map(r -> Optional.ofNullable(r)
                        .map(Response::toBuffer)
                        .map(this::deserializeState)
                        .orElse(null)));
    }

    private String getKey(TopicPartition partition) {
        return partition.topic() + ":" + partition.partition();
    }

    private <T> ProcessingState<T> deserializeState(Buffer b) {
        return Json.decodeValue(b.getDelegate(), ProcessingState.class);
    }

    @Override
    protected Uni<Void> persistProcessingState(TopicPartition partition, ProcessingState<?> state) {
        return runWithRedis(redis -> redis.send(cmd(Command.WATCH).arg(getKey(partition)))
                .chain(() -> redis.send(cmd(Command.GET).arg(getKey(partition)))
                        .map(r -> Optional.ofNullable(r)
                                .map(Response::toBuffer)
                                .map(this::deserializeState)
                                .orElse(null))
                        .chain(s -> {
                            if (s != null && s.getOffset() > state.getOffset()) {
                                log.warnf("Skipping persist operation for partition %s : higher offset found on store %d > %d",
                                        partition, s.getOffset(), state.getOffset());
                                return Uni.createFrom().voidItem();
                            } else {
                                return redis.batch(Arrays.asList(
                                        cmd(Command.MULTI),
                                        cmd(Command.SET).arg(getKey(partition)).arg(serializeState(state).toString()),
                                        cmd(Command.EXEC)))
                                        .onItem().invoke(r -> log.debugf("Persisted state for partition %s : %s -> %s",
                                                partition, state, r))
                                        .replaceWithVoid()
                                        .onFailure().recoverWithUni(t -> {
                                            log.errorf(t, "Error persisting processing state %s", state);
                                            return redis.send(cmd(Command.DISCARD)).replaceWithVoid();
                                        });
                            }
                        })));
    }

    private Buffer serializeState(ProcessingState<?> state) {
        return Buffer.newInstance(Json.encodeToBuffer(state));
    }

}
