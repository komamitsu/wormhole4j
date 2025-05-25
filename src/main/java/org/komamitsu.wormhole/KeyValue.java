package org.komamitsu.wormhole;

public class KeyValue<T> {
  private final String key;
  private final T value;

  public KeyValue(String key, T value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public T getValue() {
    return value;
  }
}
