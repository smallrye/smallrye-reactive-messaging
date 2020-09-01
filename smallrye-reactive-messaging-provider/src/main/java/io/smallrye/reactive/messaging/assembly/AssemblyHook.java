package io.smallrye.reactive.messaging.assembly;

import java.util.List;
import java.util.concurrent.Executor;

import io.smallrye.reactive.messaging.ChannelRegistar;
import io.smallrye.reactive.messaging.ChannelRegistry;

public interface AssemblyHook {

    Executor before(List<ChannelRegistar> registars, ChannelRegistry registry);

}
