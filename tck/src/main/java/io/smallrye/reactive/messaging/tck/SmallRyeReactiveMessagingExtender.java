package io.smallrye.reactive.messaging.tck;

import static io.smallrye.reactive.messaging.extension.MediatorManager.STRICT_MODE_PROPERTY;

import javax.enterprise.inject.spi.Extension;

import org.eclipse.microprofile.reactive.messaging.tck.ArchiveExtender;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;

public class SmallRyeReactiveMessagingExtender implements ArchiveExtender {
    @Override
    public void extend(JavaArchive archive) {
        System.setProperty(STRICT_MODE_PROPERTY, "true");
        archive
                .addPackages(true, ChannelRegistry.class.getPackage())
                .addAsServiceProvider(Extension.class, ReactiveMessagingExtension.class)
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }
}
