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

/**
 * A Wormhole implementation for string keys.
 *
 * @param <V> the type of values stored in this Wormhole
 */
public class WormholeForStringKey<V> extends Wormhole<String, V> {
  public WormholeForStringKey() {
    super(EncodedKeyType.STRING);
  }

  public WormholeForStringKey(int leafNodeSize) {
    super(EncodedKeyType.STRING, leafNodeSize);
  }

  public WormholeForStringKey(int leafNodeSize, boolean debugMode) {
    super(EncodedKeyType.STRING, leafNodeSize, debugMode);
  }

  @Override
  protected Object createEncodedKey(String key) {
    return key;
  }

  @Override
  protected Object createEmptyEncodedKey() {
    return "";
  }
}
