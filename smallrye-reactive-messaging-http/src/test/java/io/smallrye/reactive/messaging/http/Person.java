package io.smallrye.reactive.messaging.http;

public class Person {

  private String name;

  public String getName() {
    return name;
  }

  public Person setName(String name) {
    this.name = name;
    return this;
  }
}
