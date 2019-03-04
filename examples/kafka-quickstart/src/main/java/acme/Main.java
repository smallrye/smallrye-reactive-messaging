package acme;

import javax.enterprise.inject.se.SeContainerInitializer;

public class Main {

  public static void main(String[] args) {
    SeContainerInitializer.newInstance().initialize();
  }
}
