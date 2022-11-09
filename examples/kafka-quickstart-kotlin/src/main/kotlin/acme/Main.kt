package acme

import jakarta.enterprise.inject.se.SeContainerInitializer

object Main {

    @JvmStatic
    fun main(args: Array<String>) {
        val container = SeContainerInitializer.newInstance().initialize()

        container.beanManager.createInstance().select(BeanUsingAnEmitter::class.java).get().periodicallySendMessageToKafka()
    }
}
