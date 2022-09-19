package io.smallrye.reactive.messaging.kafka.commit;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.util.Optional;
import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.vertx.core.json.Json;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;

/**
 * Checkpointing commit handler which uses local files as state store. It creates a file per topic-partition.
 */
@Experimental("Experimental API")
public class KafkaFileCheckpointCommit extends KafkaCheckpointCommit {

    public static final String FILE_CHECKPOINT_NAME = "checkpoint-file";
    private final Vertx mutinyVertx;
    private final File stateDir;

    public KafkaFileCheckpointCommit(KafkaConnectorIncomingConfiguration config,
            Vertx vertx,
            KafkaConsumer<?, ?> consumer,
            BiConsumer<Throwable, Boolean> reportFailure,
            int defaultTimeout,
            File stateDir) {
        super(vertx, config, consumer, reportFailure, defaultTimeout);
        this.mutinyVertx = vertx;
        this.stateDir = stateDir;
    }

    @ApplicationScoped
    @Identifier(FILE_CHECKPOINT_NAME)
    public static class Factory implements KafkaCommitHandler.Factory {

        @Override
        public KafkaCommitHandler create(KafkaConnectorIncomingConfiguration config, Vertx vertx,
                KafkaConsumer<?, ?> consumer, BiConsumer<Throwable, Boolean> reportFailure) {
            int defaultTimeout = config.config()
                    .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class)
                    .orElse(60000);
            String stateDir = config.config().getValue(FILE_CHECKPOINT_NAME + ".stateDir", String.class);

            return new KafkaFileCheckpointCommit(config, vertx, consumer, reportFailure, defaultTimeout,
                    new File(stateDir));
        }
    }

    private String getStatePath(TopicPartition partition) {
        return stateDir.toPath().resolve(partition.topic() + "-" + partition.partition()).toString();
    }

    @Override
    protected Uni<ProcessingState<?>> fetchProcessingState(TopicPartition partition) {
        String statePath = getStatePath(partition);
        return mutinyVertx.fileSystem().exists(statePath).chain(exists -> {
            if (exists)
                return mutinyVertx.fileSystem().readFile(statePath)
                        .map(this::deserializeState)
                        .onFailure().invoke(t -> log.errorf(t, "Error fetching processing state for partition %s", partition))
                        .onItem().invoke(r -> log.debugf("Fetched state for partition %s : %s", partition, r));
            return Uni.createFrom().item(() -> null);
        });
    }

    private <T> ProcessingState<T> deserializeState(Buffer b) {
        return Json.decodeValue(b.getDelegate(), ProcessingState.class);
    }

    @Override
    protected Uni<Void> persistProcessingState(TopicPartition partition, ProcessingState<?> state) {
        String statePath = getStatePath(partition);
        if (state != null) {
            return mutinyVertx.fileSystem().exists(statePath).chain(exists -> {
                if (exists)
                    return fetchProcessingState(partition).onFailure().recoverWithNull();
                return mutinyVertx.fileSystem().createFile(statePath)
                        .onItem().transform(x -> (ProcessingState<?>) null)
                        .onFailure(t -> Optional.ofNullable(t.getCause())
                                .map(Object::getClass).orElse(null) == FileAlreadyExistsException.class)
                        .recoverWithNull();
            }).chain(s -> {
                if (s != null && s.getOffset() > state.getOffset()) {
                    log.warnf("Skipping persist operation : higher offset found on store %d > %d",
                            s.getOffset(), state.getOffset());
                    return Uni.createFrom().voidItem();
                } else {
                    return mutinyVertx.fileSystem().writeFile(statePath, serializeState(state));
                }
            })
                    .onFailure()
                    .invoke(t -> log.errorf(t, "Error persisting processing state `%s` for partition %s", state, partition))
                    .onItem().invoke(r -> log.debugf("Persisted state for partition %s : %s -> %s", partition, state, r));
        } else {
            return Uni.createFrom().voidItem();
        }
    }

    private Buffer serializeState(ProcessingState<?> state) {
        return Buffer.newInstance(Json.encodeToBuffer(state));
    }

}
