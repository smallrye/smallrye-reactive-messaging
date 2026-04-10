package io.smallrye.reactive.messaging.rabbitmq.reply;

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
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;
import io.smallrye.reactive.messaging.providers.helpers.ConverterUtils;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;

@EmitterFactoryFor(RabbitMQRequestReply.class)
@ApplicationScoped
// Wildcard parameterized type is not a legal managed bean
public class RabbitMQRequestReplyFactory implements EmitterFactory<RabbitMQRequestReplyImpl<Object, Object>> {

    @Inject
    ChannelRegistry channelRegistry;

    @Inject
    @Any
    RabbitMQConnector connector;

    @Inject
    // @Any would only be needed if we wanted to allow implementations with qualifiers
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
    public RabbitMQRequestReplyImpl<Object, Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return new RabbitMQRequestReplyImpl<>(configuration, defaultBufferSize, config.get(), connector, correlationIdHandlers,
                replyFailureHandlers);
    }

    @Produces
    @Typed(RabbitMQRequestReply.class)
    @Channel("")
    // Stream name is ignored during type-safe resolution
    <Req, Rep> RabbitMQRequestReply<Req, Rep> produceEmitter(InjectionPoint injectionPoint) {
        String channelName = ChannelProducer.getChannelName(injectionPoint);
        RabbitMQRequestReply<Req, Rep> emitter = channelRegistry.getEmitter(channelName, RabbitMQRequestReply.class);
        Type replyType = getReplyPayloadType(injectionPoint);
        if (replyType != null) {
            ((RabbitMQRequestReplyImpl) emitter).setReplyConverter(ConverterUtils.convertFunction(converters, replyType));
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
