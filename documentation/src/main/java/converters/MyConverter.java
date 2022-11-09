package converters;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MessageConverter;

// <code>
@ApplicationScoped
public class MyConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        // Checks whether this converter can be used to convert
        // the incoming message into a message containing a payload
        // of the type `target`.
        return in.getPayload().getClass().equals(String.class)
                && target.equals(Person.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        // Convert the incoming message into the new message.
        // It's important to build the new message **from**
        // the received one.
        return in.withPayload(new Person((String) in.getPayload()));
    }
}
// </code>
