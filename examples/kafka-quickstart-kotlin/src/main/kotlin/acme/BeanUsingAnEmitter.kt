package acme

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import javax.enterprise.context.ApplicationScoped
import javax.inject.Inject

import io.smallrye.reactive.messaging.annotations.Channel
import io.smallrye.reactive.messaging.Emitter

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
