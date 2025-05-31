package org.komamitsu.wormhole;

class KeyValue<T> {
  public final String key;
  public final T value;

  KeyValue(String key, T value) {
    this.key = key;
    this.value = value;
  }
}
