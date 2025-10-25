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
 * A Wormhole implementation for integer keys. This is experimental.
 *
 * @param <V> the type of values stored in this Wormhole
 */
public class WormholeForIntKey<V> extends WormholeBase<Integer, V> {
  public WormholeForIntKey() {
    super();
  }

  public WormholeForIntKey(int leafNodeSize) {
    super(leafNodeSize);
  }

  public WormholeForIntKey(int leafNodeSize, boolean debugMode) {
    super(leafNodeSize, debugMode);
  }

  @Override
  EncodedKey encodeKey(Integer key) {
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.putInt(key);
    return new EncodedKey(buf.array());
  }
}
