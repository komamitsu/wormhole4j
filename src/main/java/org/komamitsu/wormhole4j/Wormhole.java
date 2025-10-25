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

import java.nio.charset.StandardCharsets;

/**
 * A Wormhole implementation for string keys. This is the default Wormhole class, as support for
 * other key types is still experimental.
 *
 * @param <V> the type of values stored in this Wormhole
 */
public class Wormhole<V> extends WormholeBase<String, V> {
  public Wormhole() {
    super();
  }

  public Wormhole(int leafNodeSize) {
    super(leafNodeSize);
  }

  public Wormhole(int leafNodeSize, boolean debugMode) {
    super(leafNodeSize, debugMode);
  }

  @Override
  EncodedKey encodeKey(String key) {
    return new EncodedKey(key.getBytes(StandardCharsets.US_ASCII));
  }
}
