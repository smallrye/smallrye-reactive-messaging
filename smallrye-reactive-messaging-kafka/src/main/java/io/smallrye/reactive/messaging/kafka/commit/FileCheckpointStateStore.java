package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;

public class FileCheckpointStateStore implements CheckpointStateStore {

    public static final String STATE_STORE_NAME = "file";
    private final Vertx vertx;
    private final File stateDir;
    private final String consumerGroupId;
    private final ProcessingStateCodec codec;

    public FileCheckpointStateStore(Vertx vertx, String consumerGroupId, File stateDir, ProcessingStateCodec codec) {
        this.vertx = vertx;
        this.consumerGroupId = consumerGroupId;
        this.stateDir = stateDir;
        this.codec = codec;
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

            Optional<String> dir = config.config().getOptionalValue(
                    KafkaCommitHandler.Strategy.CHECKPOINT + "." + STATE_STORE_NAME + ".state-dir", String.class);
            File stateDir = dir.map(File::new).orElseGet(() -> {
                try {
                    return Files.createTempDirectory("io.smallrye.reactive.messaging.kafka").toFile();
                } catch (IOException e) {
                    // TODO custom exception
                    throw new IllegalStateException(e);
                }
            });

            ProcessingStateCodec stateCodec = CDIUtils.getInstanceById(stateCodecFactory, config.getChannel(), () -> {
                if (stateCodecFactory.isUnsatisfied()) {
                    return VertxJsonProcessingStateCodec.FACTORY;
                } else {
                    return stateCodecFactory.get();
                }
            }).create(stateType);
            return new FileCheckpointStateStore(vertx, consumerGroupId, stateDir, stateCodec);
        }
    }

    public File getStateDir() {
        return stateDir;
    }

    private String getStatePath(TopicPartition partition) {
        return stateDir.toPath().resolve(consumerGroupId + ":" +
                partition.topic() + ":" + partition.partition()).toString();
    }

    @Override
    public Uni<Map<TopicPartition, ProcessingState<?>>> fetchProcessingState(Collection<TopicPartition> partitions) {
        return Multi.createFrom().iterable(partitions)
                .onItem().transformToUniAndConcatenate(p -> fetchProcessingState(p).map(s -> Tuple2.of(p, s)))
                .filter(t -> t.getItem2() != null)
                .collect().asMap(Tuple2::getItem1, Tuple2::getItem2);
    }

    protected Uni<ProcessingState<?>> fetchProcessingState(TopicPartition partition) {
        String statePath = getStatePath(partition);
        return vertx.fileSystem().exists(statePath).chain(exists -> {
            if (exists)
                return vertx.fileSystem().readFile(statePath)
                        .map(this::deserializeState)
                        .onFailure().invoke(t -> log.errorf(t, "Error fetching processing state for partition %s", partition))
                        .onItem().invoke(r -> log.debugf("Fetched state for partition %s : %s", partition, r));
            return Uni.createFrom().item(() -> null);
        });
    }

    private ProcessingState<?> deserializeState(Buffer b) {
        return codec.decode(b.getBytes());
    }

    @Override
    public Uni<Void> persistProcessingState(Map<TopicPartition, ProcessingState<?>> state) {
        return Multi.createFrom().iterable(state.entrySet())
                .onItem().transformToUniAndConcatenate(e -> persistProcessingState(e.getKey(), e.getValue()))
                .collect().asList()
                .replaceWithVoid();
    }

    protected Uni<Void> persistProcessingState(TopicPartition partition, ProcessingState<?> state) {
        String statePath = getStatePath(partition);
        if (state != null) {
            return vertx.fileSystem().exists(statePath).chain(exists -> {
                if (exists)
                    return fetchProcessingState(partition).onFailure().recoverWithNull();
                return vertx.fileSystem().createFile(statePath)
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
                    return vertx.fileSystem().writeFile(statePath, serializeState(state));
                }
            })
                    .onFailure()
                    .invoke(t -> log.errorf(t, "Error persisting processing state `%s` for partition %s", state, partition))
                    .onItem().invoke(() -> log.debugf("Persisted state for partition %s : %s", partition, state));
        } else {
            return Uni.createFrom().voidItem();
        }
    }

    private Buffer serializeState(ProcessingState<?> state) {
        return Buffer.buffer(codec.encode(state));
    }

}
