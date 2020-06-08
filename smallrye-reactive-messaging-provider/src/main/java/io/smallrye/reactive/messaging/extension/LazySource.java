package io.smallrye.reactive.messaging.extension;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderLogging.log;

import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.annotations.Merge;

@SuppressWarnings({ "PublisherImplementation" })
class LazySource implements Publisher<Message<?>> {
    private PublisherBuilder<? extends Message<?>> delegate;
    private final String source;
    private final Merge.Mode mode;

    LazySource(String source, Merge.Mode mode) {
        this.source = source;
        this.mode = mode;
    }

    public void configure(ChannelRegistry registry) {
        List<PublisherBuilder<? extends Message<?>>> list = registry.getPublishers(source);
        if (!list.isEmpty()) {
            switch (mode) {
                case MERGE:
                    merge(list);
                    break;
                case ONE:
                    this.delegate = list.get(0);
                    if (list.size() > 1) {
                        log.multiplePublisherFound(source);
                    }
                    break;

                case CONCAT:
                    concat(list);
                    break;
                default:
                    throw ex.illegalArgumentMergePolicy(source, mode);
            }
        }
    }

    private void merge(List<PublisherBuilder<? extends Message<?>>> list) {
        this.delegate = ReactiveStreams.fromPublisher(Multi.createBy().merging().streams(
                list.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList())));
    }

    private void concat(List<PublisherBuilder<? extends Message<?>>> list) {
        this.delegate = ReactiveStreams.fromPublisher(Multi.createBy().concatenating().streams(
                list.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList())));
    }

    @Override
    public void subscribe(Subscriber<? super Message<?>> s) {
        delegate.to(s).run();
    }
}
