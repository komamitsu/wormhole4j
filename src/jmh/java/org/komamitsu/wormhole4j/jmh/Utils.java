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

package org.komamitsu.wormhole4j.jmh;

import static org.komamitsu.wormhole4j.jmh.Constants.*;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

final class Utils {
  private Utils() {}

  static int randomInt() {
    return ThreadLocalRandom.current().nextInt();
  }

  static int randomInt(int limit) {
    return ThreadLocalRandom.current().nextInt(limit);
  }

  static long randomLong() {
    return ThreadLocalRandom.current().nextLong();
  }

  static String randomString() {
    int keyLength = ThreadLocalRandom.current().nextInt(MIN_STRING_KEY_LEN, MAX_STRING_KEY_LEN);
    StringBuilder sb = new StringBuilder(keyLength);
    for (int j = 0; j < keyLength; j++) {
      char c = (char) ThreadLocalRandom.current().nextInt('a', 'z' + 1);
      sb.append(c);
    }
    return sb.toString();
  }

  static <T extends Comparable<T>> void iterateWithKey(
      int count, KeysState<T> keysState, Consumer<T> task) {
    List<T> keys = keysState.keys;
    for (int i = 0; i < count; i++) {
      task.accept(keys.get(i % keys.size()));
    }
  }

  static <T extends Comparable<T>> void iterateWithKeysRange(
      int count, KeysState<T> keysState, BiConsumer<T, T> task) {
    List<T> keys = keysState.keys;
    int scanSize = keysState.startIndexes.size();
    for (int i = 0; i < count; i++) {
      int index = i % scanSize;
      T k1 = keys.get(keysState.startIndexes.get(index));
      T k2 = keys.get(keysState.endIndexes.get(index));
      task.accept(k1, k2);
    }
  }
}
