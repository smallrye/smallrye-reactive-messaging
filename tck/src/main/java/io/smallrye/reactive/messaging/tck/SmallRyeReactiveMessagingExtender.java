package io.smallrye.reactive.messaging.tck;

import javax.enterprise.inject.spi.Extension;

import org.eclipse.microprofile.reactive.messaging.tck.ArchiveExtender;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;

public class SmallRyeReactiveMessagingExtender implements ArchiveExtender {
    @Override
    public void extend(JavaArchive archive) {
        archive
                .addPackages(true, ChannelRegistry.class.getPackage())
                .addAsServiceProvider(Extension.class, ReactiveMessagingExtension.class)
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }
}
