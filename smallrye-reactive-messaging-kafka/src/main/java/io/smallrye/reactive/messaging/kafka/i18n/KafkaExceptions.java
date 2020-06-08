package io.smallrye.reactive.messaging.kafka.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRRML")
public interface KafkaExceptions {

    KafkaExceptions ex = Messages.getBundle(KafkaExceptions.class);

    // 18000-18333 (exceptions)

    @Message(id = 18000, value = "`message` does not contain metadata of class %s")
    IllegalArgumentException illegalArgumentNoMetadata(Class c);

    @Message(id = 18001, value = "Unknown failure strategy: %s")
    IllegalArgumentException illegalArgumentUnknownStrategy(String strategy);

    @Message(id = 18002, value = "Expecting downstream to consume without back-pressure")
    IllegalStateException illegalStateConsumeWithoutBackPressure();

    @Message(id = 18003, value = "Only one subscriber allowed")
    IllegalStateException illegalStateOnlyOneSubscriber();

    @Message(id = 18004, value = "Invalid failure strategy: %s")
    IllegalArgumentException illegalArgumentInvalidStrategy(String strategy);
}
