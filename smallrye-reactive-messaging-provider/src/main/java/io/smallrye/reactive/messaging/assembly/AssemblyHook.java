package io.smallrye.reactive.messaging.assembly;

import java.util.List;

import io.smallrye.reactive.messaging.ChannelRegistar;
import io.smallrye.reactive.messaging.ChannelRegistry;

public interface AssemblyHook {

    void before(List<ChannelRegistar> registars, ChannelRegistry registry);

}
