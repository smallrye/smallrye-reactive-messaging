package acme;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.vertx.core.json.JsonObject;

@ApplicationScoped
public class Receiver {

    @Incoming("from-rabbitmq-string")
    public void consume(String p) {
        System.out.println("received string: " + p);
    }

    @Incoming("from-rabbitmq-jsonobject")
    public void consume(JsonObject p) {
        Price price = p.mapTo(Price.class);
        System.out.println("received jsonobject price: " + price.price);
    }

}
