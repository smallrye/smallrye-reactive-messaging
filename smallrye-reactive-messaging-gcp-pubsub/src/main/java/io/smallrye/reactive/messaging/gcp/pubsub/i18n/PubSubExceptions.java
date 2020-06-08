package io.smallrye.reactive.messaging.gcp.pubsub.i18n;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG")
public interface PubSubExceptions {

    PubSubExceptions ex = Messages.getBundle(PubSubExceptions.class);

    // 14600-14699 (exceptions)

    @Message(id = 14600, value = "Unable to build pub/sub subscription admin client")
    IllegalStateException illegalStateUnableToBuildSubscriptionAdminClient(@Cause Throwable t);

    @Message(id = 14601, value = "Unable to build pub/sub topic admin client")
    IllegalStateException illegalStateUnableToBuildTopicAdminClient(@Cause Throwable t);

    @Message(id = 14602, value = "Unable to build pub/sub publisher")
    IllegalStateException illegalStateUnableToBuildPublisher(@Cause Throwable t);

}
