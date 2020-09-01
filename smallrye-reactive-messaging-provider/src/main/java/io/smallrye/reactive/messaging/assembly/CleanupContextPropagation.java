package io.smallrye.reactive.messaging.assembly;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.context.ThreadContext;

import io.smallrye.reactive.messaging.ChannelRegistar;
import io.smallrye.reactive.messaging.ChannelRegistry;

@ApplicationScoped
public class CleanupContextPropagation implements AssemblyHook {
    @Override
    public void before(List<ChannelRegistar> registars,
            ChannelRegistry registry) {

        try {
            CleanupContextPropagation.class.getClassLoader().loadClass("org.eclipse.microprofile.context.ThreadContext");
            ThreadContext.builder().cleared(ThreadContext.ALL_REMAINING);
        } catch (Exception ignored) {
            // No context propagation.
        }
    }
}
