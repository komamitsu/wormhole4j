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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

abstract class KeysState<T extends Comparable<T>> {
  List<T> keys = new ArrayList<>(RECORD_COUNT);
  List<T> startKeys = new ArrayList<>(SCAN_OPS_COUNT);
  List<T> endKeys = new ArrayList<>(SCAN_OPS_COUNT);

  protected abstract T getRandomValue();

  protected void setupInternal() {
    for (int i = 0; i < RECORD_COUNT; i++) {
      keys.add(getRandomValue());
    }
    List<T> sortedKeys = new ArrayList<>(RECORD_COUNT);
    sortedKeys.addAll(keys);
    Collections.sort(sortedKeys);
    for (int i = 0; i < SCAN_OPS_COUNT; i++) {
      int startIndex = ThreadLocalRandom.current().nextInt(keys.size());
      int endIndex = Math.min(keys.size() - 1, startIndex + SCAN_RANGE_SIZE);
      startKeys.add(sortedKeys.get(startIndex));
      endKeys.add(sortedKeys.get(endIndex));
    }
  }
}
