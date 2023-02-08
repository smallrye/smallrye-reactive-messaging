package messages;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

@SuppressWarnings({ "unused" })
public class MessageExamples {

    public void example1() {
        Message<String> message = Message.of("hello", Metadata.of(new MyMetadata()));
        // <message>
        String payload = message.getPayload();
        Optional<MyMetadata> metadata = message.getMetadata(MyMetadata.class);
        // </message>
    }

    public void creation() {
        Price price = new Price(20.5);
        // <creation>
        // Create a simple message wrapping a payload
        Message<Price> m1 = Message.of(price);

        // Create a message with metadata
        Message<Price> m2 = Message.of(price, Metadata.of(new PriceMetadata()));

        // Create a message with several metadata
        Message<Price> m3 = Message.of(price,
                Metadata.of(new PriceMetadata(), new MyMetadata()));

        // Create a message with an acknowledgement callback
        Message<Price> m4 = Message.of(price, () -> {
            // Called when the message is acknowledged by the next consumer.
            return CompletableFuture.completedFuture(null);
        });

        // Create a message with both metadata and acknowledgement callback
        Message<Price> m5 = Message.of(price,
                Metadata.of(new PriceMetadata()),
                () -> {
                    // Called when the message is acknowledged by the next consumer.
                    return CompletableFuture.completedFuture(null);
                });
        // </creation>
    }

    public void copy() {
        Price price = new Price(20.5);
        Message<Price> message = Message.of(price);
        // <copy>

        // Create a new message with a new payload but with the same metadata
        Message<Price> m1 = message.withPayload(new Price(12.4));

        // Create a new message with a new payload and add another metadata
        Message<Price> m2 = message
                .withPayload(new Price(15.0))
                .withMetadata(Metadata.of(new PriceMetadata()));

        // Create a new message with a new payload and a custom acknowledgement
        Message<Price> m3 = message
                .withPayload(new Price(15.0))
                .withAck(() ->
                // acknowledge the incoming message
                message.ack()
                        .thenAccept(x -> {
                            // do something
                        }));
        // </copy>
    }

    public void nullPayload() {
        // <null-payload>
        // Create a message with null payload
        Message<Price> m6 = Message.of(null, Metadata.of(new PriceMetadata()));
        // </null-payload>
    }

    public static class Price {
        final double price;

        public Price(double p) {
            this.price = p;
        }
    }

    static class PriceMetadata {
        final long timestamp;

        public PriceMetadata() {
            this.timestamp = System.currentTimeMillis();
        }
    }
}
