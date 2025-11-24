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

/**
 * A Wormhole implementation for integer keys.
 *
 * @param <V> the type of values stored in this Wormhole
 */
public class WormholeForIntKey<V> extends Wormhole<Integer, V> {
  public WormholeForIntKey() {
    super(EncodedKeyType.BYTE_ARRAY);
  }

  public WormholeForIntKey(int leafNodeSize) {
    super(EncodedKeyType.BYTE_ARRAY, leafNodeSize);
  }

  public WormholeForIntKey(int leafNodeSize, boolean debugMode) {
    super(EncodedKeyType.BYTE_ARRAY, leafNodeSize, debugMode);
  }

  @Override
  protected KeyValue<Integer, V> createKeyValue(Integer key, V value) {
    return EncodedKeyUtils.createKeyValue(
        EncodedKeyType.BYTE_ARRAY, createEncodedKey(key), key, value);
  }

  @Override
  protected Object createEncodedKey(Integer key) {
    ByteBuffer byteBuf = ByteBuffer.allocate(4);
    byteBuf.putInt(key ^ 0x80000000);
    return new ByteArray(byteBuf.array());
  }

  @Override
  protected Object createEmptyEncodedKey() {
    return ByteArray.EMPTY_INSTANCE;
  }
}
