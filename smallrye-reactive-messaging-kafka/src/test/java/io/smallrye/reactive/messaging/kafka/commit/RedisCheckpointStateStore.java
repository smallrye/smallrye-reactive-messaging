package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;
import static io.vertx.mutiny.redis.client.Request.cmd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.impl.JsonHelper;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.helpers.NoStackTraceException;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.Request;
import io.vertx.mutiny.redis.client.Response;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.RedisOptionsConverter;

public class RedisCheckpointStateStore implements CheckpointStateStore {

    public static final String STATE_STORE_NAME = "redis";
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Redis redis;
    private final String consumerGroupId;
    private final ProcessingStateCodec stateCodec;

    public RedisCheckpointStateStore(Redis redis, String consumerGroupId, ProcessingStateCodec stateCodec) {
        this.redis = redis;
        this.consumerGroupId = consumerGroupId;
        this.stateCodec = stateCodec;
    }

    private <T> Uni<T> runWithRedis(Function<Redis, Uni<T>> action) {
        return Uni.createFrom().deferred(() -> {
            if (started.compareAndSet(false, true)) {
                return redis.connect().replaceWith(redis)
                        .onFailure().invoke(t -> started.set(false));
            } else {
                return Uni.createFrom().item(redis);
            }
        })
                .chain(action::apply);
    }

    @ApplicationScoped
    @Identifier(STATE_STORE_NAME)
    public static class Factory implements CheckpointStateStore.Factory {

        private final Instance<ProcessingStateCodec.Factory> stateCodecFactory;

        @Inject
        public Factory(@Any Instance<ProcessingStateCodec.Factory> stateCodecFactory) {
            this.stateCodecFactory = stateCodecFactory;
        }

        @Override
        public CheckpointStateStore create(KafkaConnectorIncomingConfiguration config, Vertx vertx,
                KafkaConsumer<?, ?> consumer, Class<?> stateType) {
            String consumerGroupId = (String) consumer.configuration().get(ConsumerConfig.GROUP_ID_CONFIG);

            JsonObject entries = JsonHelper.asJsonObject(config.config(),
                    KafkaCommitHandler.Strategy.CHECKPOINT + "." + STATE_STORE_NAME + ".");
            RedisOptions options = new RedisOptions();
            RedisOptionsConverter.fromJson(entries, options);
            Redis redis = Redis.createClient(vertx, options);

            ProcessingStateCodec stateCodec = CDIUtils.getInstanceById(stateCodecFactory, config.getChannel(), () -> {
                if (stateCodecFactory.isUnsatisfied()) {
                    return VertxJsonProcessingStateCodec.FACTORY;
                } else {
                    return stateCodecFactory.get();
                }
            }).create(stateType);
            return new RedisCheckpointStateStore(redis, consumerGroupId, stateCodec);
        }

    }

    @Override
    public void close() {
        if (started.get()) {
            redis.close();
            started.set(false);
        }
    }

    @Override
    public Uni<Map<TopicPartition, ProcessingState<?>>> fetchProcessingState(Collection<TopicPartition> partitions) {
        List<Tuple2<TopicPartition, String>> tps = partitions.stream()
                .map(tp -> Tuple2.of(tp, this.getKey(tp)))
                .collect(Collectors.toList());
        Object[] args = tps.stream().map(Tuple2::getItem2).toArray();
        return runWithRedis(redis -> redis.send(cmd(Command.MGET, args))
                .onFailure().invoke(t -> log.errorf(t, "Error fetching processing state %s", partitions))
                .onItem().invoke(r -> log.tracef("Fetched state for partitions %s : %s", partitions, r))
                .map(response -> {
                    Map<TopicPartition, ProcessingState<?>> stateMap = new HashMap<>();
                    for (int i = 0; i < tps.size(); i++) {
                        Tuple2<TopicPartition, String> t = tps.get(i);
                        final int j = i;
                        Optional.ofNullable(response)
                                .map(r -> r.get(j))
                                .map(Response::toBuffer)
                                .map(this::deserializeState)
                                .ifPresent(s -> stateMap.put(t.getItem1(), s));
                    }
                    return stateMap;
                }));
    }

    private String getKey(TopicPartition partition) {
        return consumerGroupId + ":" + partition.topic() + ":" + partition.partition();
    }

    private ProcessingState<?> deserializeState(Buffer b) {
        return stateCodec.decode(b.getBytes());
    }

    @Override
    public Uni<Void> persistProcessingState(Map<TopicPartition, ProcessingState<?>> states) {
        if (states.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        List<Tuple2<TopicPartition, String>> tps = states.keySet().stream()
                .map(tp -> Tuple2.of(tp, this.getKey(tp)))
                .collect(Collectors.toList());
        Object[] args = tps.stream().map(Tuple2::getItem2).toArray();
        return runWithRedis(redis -> redis.send(cmd(Command.WATCH, args)))
                .chain(() -> fetchProcessingState(states.keySet()))
                .chain(current -> {
                    Map<String, String> map = states.entrySet().stream()
                            .filter(toPersist -> {
                                TopicPartition key = toPersist.getKey();
                                ProcessingState<?> newState = toPersist.getValue();
                                ProcessingState<?> currentState = current.get(key);
                                return ProcessingState.isEmptyOrNull(currentState) ||
                                        (!ProcessingState.isEmptyOrNull(newState)
                                                && newState.getOffset() >= currentState.getOffset());
                            }).collect(Collectors.toMap(e -> getKey(e.getKey()), e -> serializeState(e.getValue()).toString()));
                    if (map.isEmpty()) {
                        return Uni.createFrom().voidItem();
                    } else {
                        List<Request> cmds = new ArrayList<>();
                        cmds.add(cmd(Command.MULTI));
                        Request mset = cmd(Command.MSET);
                        map.forEach((t, s) -> mset.arg(t).arg(s));
                        cmds.add(mset);
                        cmds.add(cmd(Command.EXEC));
                        return redis.batch(cmds).chain(responses -> {
                            if (responses.contains(null)) {
                                return Uni.createFrom().failure(new AbortedException("Redis batch aborted"));
                            } else {
                                return Uni.createFrom().voidItem();
                            }
                        }).onItem().invoke(r -> log.debugf("Persisted state for partition %s -> %s", map, r));
                    }
                });
    }

    private Buffer serializeState(ProcessingState<?> state) {
        return Buffer.buffer(stateCodec.encode(state));
    }

    public static class AbortedException extends NoStackTraceException {

        public AbortedException(String msg) {
            super(msg);
        }
    }

}
