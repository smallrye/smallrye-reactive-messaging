package io.smallrye.reactive.messaging.tck;

import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.extension.ReactiveMessagingExtension;
import org.eclipse.microprofile.reactive.messaging.tck.ArchiveExtender;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

import javax.enterprise.inject.spi.Extension;

public class SmallRyeReactiveMessagingExtender implements ArchiveExtender {
  @Override
  public void extend(JavaArchive archive) {
    archive
      .addPackages(true, StreamRegistry.class.getPackage())
      .addAsServiceProvider(Extension.class, ReactiveMessagingExtension.class)
      .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
  }
}

