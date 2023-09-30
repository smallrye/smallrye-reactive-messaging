package io.smallrye.reactive.messaging.aws.sqs.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG", length = 5)
public interface SqsExceptions {

    SqsExceptions ex = Messages.getBundle(SqsExceptions.class);

    @Message(id = 19100, value = "Unable to build Pulsar client")
    IllegalStateException illegalStateUnableToBuildClient(@Cause Throwable t);

    @Message(id = 19101, value = "Unable to build Pulsar consumer")
    IllegalStateException illegalStateUnableToBuildConsumer(@Cause Throwable t);

    @Message(id = 19102, value = "Unable to build Pulsar producer")
    IllegalStateException illegalStateUnableToBuildProducer(@Cause Throwable t);

    @Message(id = 19103, value = "Expecting downstream to consume without back-pressure")
    IllegalStateException illegalStateConsumeWithoutBackPressure();

    @Message(id = 19104, value = "Only one subscriber allowed")
    IllegalStateException illegalStateOnlyOneSubscriber();
}
