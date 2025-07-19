package org.komamitsu.wormhole;

import java.util.Objects;

class KeyValue<T> {
  final String key;
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

  T getValue() {
    return value;
  }

  void setValue(T value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyValue<?> keyValue = (KeyValue<?>) o;
    return Objects.equals(key, keyValue.key) && Objects.equals(value, keyValue.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }
}
