package acme

import org.eclipse.microprofile.reactive.messaging.Channel
import org.eclipse.microprofile.reactive.messaging.Emitter
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject


@ApplicationScoped
open class BeanUsingAnEmitter {

    @Inject
    @Channel("my-channel")
    private lateinit var emitter: Emitter<String>

    open fun periodicallySendMessageToKafka() {
        val counter = AtomicInteger()
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate({ emitter.send("Hello " + counter.getAndIncrement()) },
                        1, 1, TimeUnit.SECONDS)
    }

}
