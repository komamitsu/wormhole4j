package org.komamitsu.wormhole;

public class KeyValue<T> {
  public final String key;
  private T value;

  KeyValue(String key, T value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return "KeyValue{" +
        "key='" + Utils.printableKey(key) + '\'' +
        ", value=" + value +
        '}';
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }
}
