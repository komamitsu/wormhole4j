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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * A Wormhole implementation for string keys. This is the default Wormhole class, as support for
 * other key types is still experimental.
 *
 * @param <V> the type of values stored in this Wormhole
 */
public class WormholeForIntKey<V> extends WormholeBase<Integer, V> {
  public WormholeForIntKey() {
    super(EncodedKeyType.BYTE_ARRAY);
  }

  public WormholeForIntKey(int leafNodeSize) {
    super(EncodedKeyType.BYTE_ARRAY, leafNodeSize);
  }

  public WormholeForIntKey(int leafNodeSize, boolean debugMode) {
    super(EncodedKeyType.BYTE_ARRAY, leafNodeSize, debugMode);
  }

  public void put(Integer key, V value) {
    putInternal(createEncodedKey(key), key, value);
  }

  public boolean delete(Integer key) {
    return deleteInternal(createEncodedKey(key));
  }

  public V get(Integer key) {
    return getInternal(createEncodedKey(key));
  }

  public List<KeyValue<Integer, V>> scanWithCount(Integer startKey, int count) {
    return scanWithCountInternal(createEncodedKey(startKey), count);
  }

  public void scan(
      @Nullable Integer startKey,
      @Nullable Integer endKey,
      boolean isEndKeyExclusive,
      Function<KeyValue<Integer, V>, Boolean> function) {
    scanInternal(
        startKey == null ? ByteArray.EMPTY_INSTANCE : createEncodedKey(startKey),
        endKey == null ? null : createEncodedKey(endKey),
        isEndKeyExclusive,
        null,
        function);
  }

  KeyValue<Integer, V> createKey(Integer key, V value) {
    return EncodedKeyUtils.createKeyValue(
        EncodedKeyType.BYTE_ARRAY, createEncodedKey(key), key, value);
  }

  private ByteArray createEncodedKey(int key) {
    ByteBuffer byteBuf = ByteBuffer.allocate(4);
    byteBuf.putInt(key ^ 0x80000000);
    return new ByteArray(byteBuf.array());
  }
}
