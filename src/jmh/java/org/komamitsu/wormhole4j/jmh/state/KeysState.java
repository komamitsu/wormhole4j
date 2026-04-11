/*
 * Copyright 2026 Mitsunori Komatsu
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

package org.komamitsu.wormhole4j.jmh.state;

import static org.komamitsu.wormhole4j.jmh.Constants.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

public abstract class KeysState<T extends Comparable<T>> {
  public List<T> keys;
  public List<T> startKeys;
  public List<T> endKeys;

  protected abstract T createRandomValue();

  private int getRandomKeyIndex() {
    return ThreadLocalRandom.current().nextInt(keys.size());
  }

  private int getRandomScanKeyIndex() {
    return ThreadLocalRandom.current().nextInt(startKeys.size());
  }

  public T getRandomKey() {
    return keys.get(getRandomKeyIndex());
  }

  public void withRandomKeyRange(BiConsumer<T, T> task) {
    int keyIndex = getRandomScanKeyIndex();
    task.accept(startKeys.get(keyIndex), endKeys.get(keyIndex));
  }

  protected void setupInternal() {
    keys = new ArrayList<>(RECORD_COUNT);
    startKeys = new ArrayList<>(SCAN_OPS_COUNT);
    endKeys = new ArrayList<>(SCAN_OPS_COUNT);
    for (int i = 0; i < RECORD_COUNT; i++) {
      keys.add(createRandomValue());
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
