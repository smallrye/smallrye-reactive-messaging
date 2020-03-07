package io.smallrye.reactive.messaging.extension;

import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.annotations.Merge;

@SuppressWarnings({ "rawtypes", "PublisherImplementation" })
class LazySource implements Publisher<Message> {
    private PublisherBuilder<? extends Message> delegate;
    private String source;
    private Merge.Mode mode;

    LazySource(String source, Merge.Mode mode) {
        this.source = source;
        this.mode = mode;
    }

    public void configure(ChannelRegistry registry, Logger logger) {
        List<PublisherBuilder<? extends Message>> list = registry.getPublishers(source);
        if (!list.isEmpty()) {
            switch (mode) {
                case MERGE:
                    merge(list);
                    break;
                case ONE:
                    this.delegate = list.get(0);
                    if (list.size() > 1) {
                        logger
                                .warn("Multiple publisher found for {}, using the merge policy `ONE` takes the first found",
                                        source);
                    }
                    break;

                case CONCAT:
                    concat(list);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown merge policy for " + source + ": " + mode);
            }
        }
    }

    private void merge(List<PublisherBuilder<? extends Message>> list) {
        this.delegate = ReactiveStreams.fromPublisher(Multi.createBy().merging().streams(
                list.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList())));
    }

    private void concat(List<PublisherBuilder<? extends Message>> list) {
        this.delegate = ReactiveStreams.fromPublisher(Multi.createBy().concatenating().streams(
                list.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList())));
    }

    @Override
    public void subscribe(Subscriber<? super Message> s) {
        delegate.to(s).run();
    }
}
