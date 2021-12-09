package io.smallrye.reactive.messaging.kafka.companion;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Multi;

/**
 * Abstract task for consuming or producing Kafka records provided by the given {@link Multi}
 * <p>
 * This class leverages {@link RecordsSubscriber} to subscribe itself to the given multi.
 *
 * @param <T> the type of items
 * @param <SELF> the reference to self type
 */
public abstract class KafkaTask<T, SELF extends KafkaTask<T, SELF>> extends RecordsSubscriber<T, SELF> implements Iterable<T> {

    /**
     * The {@link Multi} to subscribe
     */
    private final Multi<T> multi;

    /**
     * Create a new {@link KafkaTask}
     *
     * @param multi the multi to subscribe to
     */
    public KafkaTask(Multi<T> multi) {
        super(Long.MAX_VALUE);
        this.multi = multi;
        start();
    }

    private void start() {
        if (!hasSubscribed()) {
            this.multi.subscribe(this);
        }
    }

    public Multi<T> getMulti() {
        return multi;
    }

    public SELF stop() {
        return cancel();
    }

    protected abstract long offset(T record);

    protected abstract TopicPartition topicPartition(T record);

    public long firstOffset() {
        T firstRecord = getFirstRecord();
        if (firstRecord == null) {
            return -1;
        }
        return offset(firstRecord);
    }

    public long lastOffset() {
        T lastRecord = getLastRecord();
        if (lastRecord == null) {
            return -1;
        }
        return offset(lastRecord);
    }

    public Map<TopicPartition, List<T>> byTopicPartition() {
        return getRecords().stream().collect(Collectors.groupingBy(this::topicPartition));
    }

}
