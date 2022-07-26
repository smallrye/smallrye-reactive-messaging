package emitter;

import java.util.concurrent.Flow.Publisher;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.EmitterType;
import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;

public class EmitterFactoryExamples {

    // <custom-emitter-declaration>

    public interface CustomEmitter<T> extends EmitterType {

        <M extends Message<? extends T>> void sendAndForget(M msg);

    }

    public static class CustomEmitterImpl<T> implements CustomEmitter<T>, MessagePublisherProvider<Object> {

        Publisher<Message<?>> publisher;

        public CustomEmitterImpl(EmitterConfiguration configuration, long defaultBufferSize) {
            //... initialize emitter with configuration
        }

        @Override
        public Publisher<Message<?>> getPublisher() {
            return publisher;
        }

        @Override
        public <M extends Message<? extends T>> void sendAndForget(M msg) {
            //... send to stream
        }
    }
    // </custom-emitter-declaration>

    // <custom-emitter-factory>
    @EmitterFactoryFor(CustomEmitter.class)
    @ApplicationScoped
    public static class CustomEmitterFactory implements EmitterFactory<CustomEmitterImpl<Object>> {

        @Inject
        ChannelRegistry channelRegistry;

        @Override
        public CustomEmitterImpl<Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
            return new CustomEmitterImpl<>(configuration, defaultBufferSize);
        }

        @Produces
        @Channel("") // Stream name is ignored during type-safe resolution
        <T> CustomEmitter<T> produce(InjectionPoint injectionPoint) {
            String channelName = ChannelProducer.getChannelName(injectionPoint);
            return channelRegistry.getEmitter(channelName, CustomEmitter.class);
        }
    }
    // </custom-emitter-factory>

    // <custom-emitter-usage>

    @Inject
    @Channel("custom-emitter-channel")
    CustomEmitter<String> customEmitter;

    //...

    public void emitMessage() {
        customEmitter.sendAndForget(Message.of("a"));
        customEmitter.sendAndForget(Message.of("b"));
        customEmitter.sendAndForget(Message.of("c"));
    }
    // </custom-emitter-usage>

}
