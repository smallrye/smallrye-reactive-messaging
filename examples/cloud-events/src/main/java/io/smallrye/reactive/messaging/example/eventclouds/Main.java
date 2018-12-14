package io.smallrye.reactive.messaging.example.eventclouds;

import javax.enterprise.inject.se.SeContainerInitializer;

public class Main {

  public static void main(String[] args) throws InterruptedException {
    SeContainerInitializer instance = SeContainerInitializer.newInstance();
    instance.initialize();

    Thread.sleep(10000);
  }


}
