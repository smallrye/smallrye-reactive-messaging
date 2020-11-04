package io.smallrye.reactive.messaging.kafka.i18n;

import org.jboss.logging.Messages;
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

}
