package io.smallrye.reactive.messaging.kafka.i18n;

import javax.enterprise.inject.AmbiguousResolutionException;
import javax.enterprise.inject.UnsatisfiedResolutionException;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for Kafka Connector
 * Assigned ID range is 18000-18099
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface KafkaExceptions {

    KafkaExceptions ex = Messages.getBundle(KafkaExceptions.class);

    @Message(id = 18000, value = "`message` does not contain metadata of class %s")
    IllegalArgumentException illegalArgumentNoMetadata(Class c);

    @Message(id = 18001, value = "Unknown failure strategy: %s")
    IllegalArgumentException illegalArgumentUnknownFailureStrategy(String strategy);

    @Message(id = 18002, value = "Expecting downstream to consume without back-pressure")
    IllegalStateException illegalStateConsumeWithoutBackPressure();

    @Message(id = 18003, value = "Only one subscriber allowed")
    IllegalStateException illegalStateOnlyOneSubscriber();

    @Message(id = 18004, value = "Invalid failure strategy: %s")
    IllegalArgumentException illegalArgumentInvalidFailureStrategy(String strategy);

    @Message(id = 18005, value = "Unknown commit strategy: %s")
    IllegalArgumentException illegalArgumentUnknownCommitStrategy(String strategy);

    @Message(id = 18006, value = "Invalid commit strategy: %s")
    IllegalArgumentException illegalArgumentInvalidCommitStrategy(String strategy);

    @Message(id = 18007, value = "Unable to find the KafkaConsumerRebalanceListener named `%s` for channel `%s`")
    UnsatisfiedResolutionException unableToFindRebalanceListener(String name, String channel);

    @Message(id = 18008, value = "Unable to select the KafkaConsumerRebalanceListener named `%s` for channel `%s` - too many matches (%d)")
    AmbiguousResolutionException unableToFindRebalanceListener(String name, String channel, int count);

    @Message(id = 18009, value = "Cannot configure the Kafka consumer for channel `%s` - the `mp.messaging.incoming.%s.value.deserializer` property is missing")
    IllegalArgumentException missingValueDeserializer(String channel, String channelAgain);

    @Message(id = 18010, value = "Unable to create an instance of `%s`")
    IllegalArgumentException unableToCreateInstance(String clazz, @Cause Throwable cause);

    @Message(id = 18011, value = "Unable to find the DeserializationFailureHandler named `%s` for channel `%s`")
    UnsatisfiedResolutionException unableToFindDeserializationFailureHandler(String name, String channel);

    @Message(id = 18012, value = "Unable to select the DeserializationFailureHandler named `%s` for channel `%s` - too many matches (%d)")
    AmbiguousResolutionException unableToFindDeserializationFailureHandler(String name, String channel, int count);

    @Message(id = 18013, value = "Cannot configure the Kafka producer for channel `%s` - the `mp.messaging.outgoing.%s.value.serializer` property is missing")
    IllegalArgumentException missingValueSerializer(String channel, String channelAgain);
}
