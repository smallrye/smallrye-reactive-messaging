package emitter;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

// <intro>
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class MyImperativeBean {

    @Inject
    @Channel("prices")
    Emitter<Double> emitter;

    // ...

    public void send(double d) {
        emitter.send(d);
    }
}

// </intro>
