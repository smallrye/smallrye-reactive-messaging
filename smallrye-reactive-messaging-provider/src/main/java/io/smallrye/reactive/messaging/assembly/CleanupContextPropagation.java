package io.smallrye.reactive.messaging.assembly;

import java.util.List;
import java.util.concurrent.Executor;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.context.ThreadContext;

import io.smallrye.reactive.messaging.ChannelRegistar;
import io.smallrye.reactive.messaging.ChannelRegistry;

@ApplicationScoped
public class CleanupContextPropagation implements AssemblyHook {
    @Override
    public Executor before(List<ChannelRegistar> registars,
            ChannelRegistry registry) {

        try {
            CleanupContextPropagation.class.getClassLoader().loadClass("org.eclipse.microprofile.context.ThreadContext");
            System.out.println("Context Propgation hook called and clearing context...");
            return ThreadContext.builder().unchanged().propagated().cleared(ThreadContext.ALL_REMAINING).build().currentContextExecutor();
        } catch (Exception ignored) {
            // No context propagation.
            return null;
        }
    }
}
