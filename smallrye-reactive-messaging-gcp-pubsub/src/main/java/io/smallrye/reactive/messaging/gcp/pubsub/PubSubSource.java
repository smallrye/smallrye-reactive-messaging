package io.smallrye.reactive.messaging.gcp.pubsub;

import static io.smallrye.reactive.messaging.gcp.pubsub.i18n.PubSubMessages.msg;

import java.util.Objects;
import java.util.function.Consumer;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.gcp.pubsub.tracing.PubSubOpenTelemetryInstrumenter;

public class PubSubSource implements Consumer<MultiEmitter<? super Message<?>>> {

    private final PubSubConfig config;

    private final PubSubManager manager;

    private final PubSubOpenTelemetryInstrumenter incomingInstrumenter;

    public PubSubSource(final PubSubConfig config, final PubSubManager manager,
            final PubSubOpenTelemetryInstrumenter incomingInstrumenter) {
        this.config = Objects.requireNonNull(config, msg.isRequired("config"));
        this.manager = Objects.requireNonNull(manager, msg.isRequired("manager"));
        this.incomingInstrumenter = incomingInstrumenter;
    }

    @Override
    public void accept(MultiEmitter<? super Message<?>> emitter) {
        manager.subscriber(config, emitter, incomingInstrumenter);
    }

}
