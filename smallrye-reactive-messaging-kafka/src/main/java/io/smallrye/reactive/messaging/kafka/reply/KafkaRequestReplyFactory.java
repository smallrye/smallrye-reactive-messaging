package io.smallrye.reactive.messaging.kafka.reply;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.Typed;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.smallrye.reactive.messaging.kafka.fault.KafkaFailureHandler;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.providers.helpers.ConverterUtils;

@EmitterFactoryFor(KafkaRequestReply.class)
@ApplicationScoped
// Wildcard parameterized type is not a legal managed bean
public class KafkaRequestReplyFactory implements EmitterFactory<KafkaRequestReplyImpl<Object, Object>> {

    @Inject
    ChannelRegistry channelRegistry;

    @Inject
    ExecutionHolder holder;

    @Inject
    KafkaCDIEvents kafkaCDIEvents;

    @Inject
    // @Any would only be needed if we wanted to allow implementations with qualifiers
    Instance<MessageConverter> converters;

    @Inject
    @Any
    Instance<DeserializationFailureHandler<?>> failureHandlers;

    @Inject
    @Any
    Instance<KafkaCommitHandler.Factory> commitStrategyFactories;

    @Inject
    @Any
    Instance<KafkaFailureHandler.Factory> failureStrategyFactories;
    @Inject
    @Any
    Instance<KafkaConsumerRebalanceListener> rebalanceListeners;
    @Inject
    @Any
    Instance<CorrelationIdHandler> correlationIdHandlers;
    @Inject
    @Any
    Instance<ReplyFailureHandler> replyFailureHandlers;

    @Inject
    Instance<Config> config;

    @Inject
    @Any
    Instance<Map<String, Object>> configurations;

    @Inject
    Instance<OpenTelemetry> openTelemetryInstance;

    @Override
    public KafkaRequestReplyImpl<Object, Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return new KafkaRequestReplyImpl<>(configuration, defaultBufferSize, config.get(), configurations, holder.vertx(),
                kafkaCDIEvents, openTelemetryInstance, commitStrategyFactories, failureStrategyFactories, failureHandlers,
                correlationIdHandlers, replyFailureHandlers, rebalanceListeners);
    }

    @Produces
    @Typed(KafkaRequestReply.class)
    @Channel("")
    // Stream name is ignored during type-safe resolution
    <Req, Rep> KafkaRequestReply<Req, Rep> produceEmitter(InjectionPoint injectionPoint) {
        String channelName = ChannelProducer.getChannelName(injectionPoint);
        KafkaRequestReply<Req, Rep> emitter = channelRegistry.getEmitter(channelName, KafkaRequestReply.class);
        Type replyType = getReplyPayloadType(injectionPoint);
        if (replyType != null) {
            ((KafkaRequestReplyImpl) emitter).setReplyConverter(ConverterUtils.convertFunction(converters, replyType));
        }
        return emitter;
    }

    private Type getReplyPayloadType(InjectionPoint injectionPoint) {
        if (injectionPoint.getType() instanceof ParameterizedType) {
            Type[] typeArguments = ((ParameterizedType) injectionPoint.getType()).getActualTypeArguments();
            if (typeArguments.length == 2) {
                return typeArguments[1];
            }
        }
        return null;
    }

}
