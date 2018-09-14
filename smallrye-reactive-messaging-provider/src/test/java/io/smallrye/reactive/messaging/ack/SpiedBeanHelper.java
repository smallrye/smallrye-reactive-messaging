package io.smallrye.reactive.messaging.ack;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class SpiedBeanHelper {

  private class Entry {
    final String value;
    final long timeStamp;

    public Entry(String value) {
      this.value = value;
      this.timeStamp = System.nanoTime();
    }

    String value() {
      return value;
    }

    long timestamp() {
      return timeStamp;
    }
  }

  private Map<String, List<Entry>> processed = new ConcurrentHashMap<>();
  private Map<String, List<Entry>> acknowledged = new ConcurrentHashMap<>();

  public List<String> received(String key) {
    if (processed.get(key) == null) {
      return null;
    }
    return processed.get(key).stream().map(Entry::value).collect(Collectors.toList());
  }

  public List<String> acknowledged(String key) {
    if (acknowledged.get(key) == null) {
      return null;
    }
    return acknowledged.get(key).stream().map(Entry::value).collect(Collectors.toList());
  }

  public List<Long> receivedTimeStamps(String key) {
    return processed.get(key).stream().map(Entry::timestamp).collect(Collectors.toList());
  }

  public List<Long> acknowledgeTimeStamps(String key) {
    return acknowledged.get(key).stream().map(Entry::timestamp).collect(Collectors.toList());
  }

  protected void processed(String name, String value) {
    processed.computeIfAbsent(name, x -> new CopyOnWriteArrayList<>()).add(new Entry(value));
  }

  protected void acknowledged(String name, String value) {
    acknowledged.computeIfAbsent(name, x -> new CopyOnWriteArrayList<>()).add(new Entry(value));
  }

  protected void microNap() {
    try {
      Thread.sleep(1);
    } catch (Exception e) {
      // Ignore me.
    }
  }

  protected void nap() {
    try {
      Thread.sleep(10);
    } catch (Exception e) {
      // Ignore me.
    }
  }
}
