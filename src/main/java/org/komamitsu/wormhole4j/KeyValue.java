/*
 * Copyright 2025 Mitsunori Komatsu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.wormhole4j;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a key-value pair stored in a {@link WormholeBase}.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class KeyValue<K, V> {
  final K key;
  V value;
  private final EncodedKeyType encodedKeyType;
  private final String encodedStringKey;
  private final byte[] encodedByteArrayKey;

  KeyValue(String encodedStringKey, K key, V value) {
    this.encodedKeyType = EncodedKeyType.STRING;
    this.encodedStringKey = encodedStringKey;
    this.encodedByteArrayKey = null;
    this.key = key;
    this.value = value;
  }

  KeyValue(byte[] encodedByteArrayKey, K key, V value) {
    this.encodedKeyType = EncodedKeyType.BYTE_ARRAY;
    this.encodedStringKey = null;
    this.encodedByteArrayKey = encodedByteArrayKey;
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    switch (encodedKeyType) {
      case STRING:
        return String.format(
            "KeyValue{key=%s, value=%s, encodedKeyType=%s, encodedString=%s}",
            key, value, encodedKeyType, encodedStringKey);
      case BYTE_ARRAY:
        assert encodedByteArrayKey != null;
        return String.format(
            "KeyValue{key=%s, value=%s, encodedKeyType=%s, encodedByteArray=%s}",
            key, value, encodedKeyType, ByteArrayUtils.toString(encodedByteArrayKey));
      default:
        throw new AssertionError();
    }
  }

  /**
   * Returns the key.
   *
   * @return the key
   */
  public K getKey() {
    return key;
  }

  /**
   * Returns the encoded key.
   *
   * @return the encoded key
   */
  Object getEncodedKey() {
    switch (encodedKeyType) {
      case STRING:
        return encodedStringKey;
      case BYTE_ARRAY:
        return encodedByteArrayKey;
      default:
        throw new AssertionError();
    }
  }

  /**
   * Returns the value.
   *
   * @return the value
   */
  public V getValue() {
    return value;
  }

  void setValue(V value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) return false;
    KeyValue<?, ?> keyValue = (KeyValue<?, ?>) o;
    return Objects.equals(key, keyValue.key)
        && Objects.equals(value, keyValue.value)
        && encodedKeyType == keyValue.encodedKeyType
        && Objects.equals(encodedStringKey, keyValue.encodedStringKey)
        && Objects.deepEquals(encodedByteArrayKey, keyValue.encodedByteArrayKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        key, value, encodedKeyType, encodedStringKey, Arrays.hashCode(encodedByteArrayKey));
  }
}
