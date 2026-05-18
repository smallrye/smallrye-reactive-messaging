package io.smallrye.reactive.messaging.amqp.reply;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.Typed;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.providers.helpers.ConverterUtils;

@EmitterFactoryFor(AmqpRequestReply.class)
@ApplicationScoped
public class AmqpRequestReplyFactory implements EmitterFactory<AmqpRequestReplyImpl<Object, Object>> {

    @Inject
    ChannelRegistry channelRegistry;

    @Inject
    @Any
    AmqpConnector connector;

    @Inject
    Instance<MessageConverter> converters;

    @Inject
    @Any
    Instance<CorrelationIdHandler> correlationIdHandlers;

    @Inject
    @Any
    Instance<ReplyFailureHandler> replyFailureHandlers;

    @Inject
    Instance<Config> config;

    @Override
    public AmqpRequestReplyImpl<Object, Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return new AmqpRequestReplyImpl<>(configuration, defaultBufferSize, config.get(), connector, correlationIdHandlers,
                replyFailureHandlers);
    }

    @Produces
    @Typed(AmqpRequestReply.class)
    @Channel("")
    <Req, Rep> AmqpRequestReply<Req, Rep> produceEmitter(InjectionPoint injectionPoint) {
        String channelName = ChannelProducer.getChannelName(injectionPoint);
        AmqpRequestReply<Req, Rep> emitter = channelRegistry.getEmitter(channelName, AmqpRequestReply.class);
        Type replyType = getReplyPayloadType(injectionPoint);
        if (replyType != null) {
            ((AmqpRequestReplyImpl) emitter).setReplyConverter(ConverterUtils.convertFunction(converters, replyType));
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
