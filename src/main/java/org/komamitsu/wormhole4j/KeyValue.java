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

import java.util.Objects;

/**
 * Represents a key-value pair stored in a {@link Wormhole}.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public final class KeyValue<K, V> {
  private final K key;
  private final V value;

  KeyValue(K key, V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return String.format("KeyValue{key=%s, value=%s}", key, value);
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
   * Returns the value.
   *
   * @return the value
   */
  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) return false;
    KeyValue<?, ?> keyValue = (KeyValue<?, ?>) o;
    return Objects.equals(key, keyValue.key) && Objects.equals(value, keyValue.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }
}
