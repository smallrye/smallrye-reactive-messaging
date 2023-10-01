package io.smallrye.reactive.messaging.aws.i18n;

import jakarta.enterprise.inject.AmbiguousResolutionException;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

@MessageBundle(projectCode = "SRMSG", length = 5)
public interface AwsExceptions {

    AwsExceptions ex = Messages.getBundle(AwsExceptions.class);

    @Message(id = 18012, value = "Unable to select the Deserializer named `%s` for channel `%s` - too many matches (%d)")
    AmbiguousResolutionException unableToFindDeserializer(String name, String channel, int count);

    @Message(id = 18014, value = "Unable to select the Serializer named `%s` for channel `%s` - too many matches (%d)")
    AmbiguousResolutionException unableToFindSerializer(String name, String channel, int count);
}
