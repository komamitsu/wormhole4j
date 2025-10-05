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

import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

class BenchmarkForIntKey extends Benchmark {
  private int getRandomKey() {
    return ThreadLocalRandom.current().nextInt();
  }

  @Test
  void insertToWormhole() throws Throwable {
    execute(
        new TestCase<List<Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Insert to Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(getRandomKey());
            }
            return keys;
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(List<Integer> keys) {
            return () -> {
              WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>();
              for (int i = 0; i < recordCount; i++) {
                wormhole.put(keys.get(i), i);
              }
            };
          }
        });
  }

  @Test
  void insertToRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<List<Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Insert to Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(getRandomKey());
            }
            return keys;
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(List<Integer> keys) {
            return () -> {
              TreeMap<Integer, Integer> map = new TreeMap<>();
              for (int i = 0; i < recordCount; i++) {
                map.put(keys.get(i), i);
              }
            };
          }
        });
  }

  @Test
  void insertToAVLTreeMap() throws Throwable {
    execute(
        new TestCase<List<Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Insert to AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(getRandomKey());
            }
            return keys;
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(List<Integer> keys) {
            return () -> {
              Object2ObjectSortedMap<Integer, Integer> map = new Object2ObjectAVLTreeMap<>();
              for (int i = 0; i < recordCount; i++) {
                map.put(keys.get(i), i);
              }
            };
          }
        });
  }

  @Test
  void getFromWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<WormholeForIntKey<Integer>, Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Get from Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<WormholeForIntKey<Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              wormhole.put(key, i);
            }
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<WormholeForIntKey<Integer>, Integer> resourceAndKeys) {
            return () -> {
              WormholeForIntKey<Integer> wormhole = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(keys.size());
                wormhole.get(keys.get(keyIndex));
              }
            };
          }
        });
  }

  @Test
  void getFromRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<Integer, Integer>, Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Get from Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<TreeMap<Integer, Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            TreeMap<Integer, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<Integer, Integer>, Integer> resourceAndKeys) {
            return () -> {
              TreeMap<Integer, Integer> map = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(keys.size());
                map.get(keys.get(keyIndex));
              }
            };
          }
        });
  }

  @Test
  void getFromAVLTreeMap() throws Throwable {
    execute(
        new TestCase<
            ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer>,
            RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Get from AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<Integer, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<Integer, Integer> map = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(keys.size());
                map.get(keys.get(keyIndex));
              }
            };
          }
        });
  }

  @Test
  void updateWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<WormholeForIntKey<Integer>, Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Update Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<WormholeForIntKey<Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              wormhole.put(key, i);
            }
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<WormholeForIntKey<Integer>, Integer> resourceAndKeys) {
            return () -> {
              WormholeForIntKey<Integer> wormhole = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(keys.size());
                wormhole.put(keys.get(keyIndex), i);
              }
            };
          }
        });
  }

  @Test
  void updateRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<Integer, Integer>, Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Update Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<TreeMap<Integer, Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            TreeMap<Integer, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<Integer, Integer>, Integer> resourceAndKeys) {
            return () -> {
              TreeMap<Integer, Integer> map = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(keys.size());
                map.put(keys.get(keyIndex), i);
              }
            };
          }
        });
  }

  @Test
  void updateAVLTreeMap() throws Throwable {
    execute(
        new TestCase<
            ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer>,
            RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Update AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<Integer, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<Integer, Integer> map = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex = ThreadLocalRandom.current().nextInt(keys.size());
                map.put(keys.get(keyIndex), i);
              }
            };
          }
        });
  }

  @Test
  void deleteFromWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<WormholeForIntKey<Integer>, Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Delete from Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public boolean initForEachAttempt() {
            return true;
          }

          @Override
          public ResourceAndKeys<WormholeForIntKey<Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              wormhole.put(key, i);
            }
            Collections.shuffle(keys);
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<WormholeForIntKey<Integer>, Integer> resourceAndKeys) {
            return () -> {
              WormholeForIntKey<Integer> wormhole = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int key = keys.get(i);
                wormhole.delete(key);
              }
            };
          }
        });
  }

  @Test
  void deleteFromRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<Integer, Integer>, Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Delete from Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public boolean initForEachAttempt() {
            return true;
          }

          @Override
          public ResourceAndKeys<TreeMap<Integer, Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            TreeMap<Integer, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            Collections.shuffle(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<Integer, Integer>, Integer> resourceAndKeys) {
            return () -> {
              TreeMap<Integer, Integer> map = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int key = keys.get(i);
                map.remove(key);
              }
            };
          }
        });
  }

  @Test
  void deleteFromAVLTreeMap() throws Throwable {
    execute(
        new TestCase<
            ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer>,
            RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Delete from AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public boolean initForEachAttempt() {
            return true;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<Integer, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            Collections.shuffle(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<Integer, Integer> map = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int key = keys.get(i);
                map.remove(key);
              }
            };
          }
        });
  }

  @Test
  void scanFromWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<WormholeForIntKey<Integer>, Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Scan from Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<WormholeForIntKey<Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              wormhole.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<WormholeForIntKey<Integer>, Integer> resourceAndKeys) {
            return () -> {
              WormholeForIntKey<Integer> wormhole = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 =
                    Math.min(
                        keys.size() - 1,
                        keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                int key1 = keys.get(keyIndex1);
                int key2 = keys.get(keyIndex2);
                wormhole.scanWithExclusiveEndKey(key1, key2, kv -> true);
              }
            };
          }
        });
  }

  @Test
  void scanFromRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<Integer, Integer>, Integer>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Scan from Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<TreeMap<Integer, Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            TreeMap<Integer, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<Integer, Integer>, Integer> resourceAndKeys) {
            return () -> {
              TreeMap<Integer, Integer> map = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 =
                    Math.min(
                        keys.size() - 1,
                        keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                int key1 = keys.get(keyIndex1);
                int key2 = keys.get(keyIndex2);
                for (Map.Entry<Integer, Integer> ignored : map.subMap(key1, key2).entrySet()) {
                  // Nothing to do.
                }
              }
            };
          }
        });
  }

  @Test
  void scanFromAVLTreeMap() throws Throwable {
    execute(
        new TestCase<
            ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer>,
            RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Scan from AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer> init() {
            List<Integer> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<Integer, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              int key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<Integer, Integer>, Integer> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<Integer, Integer> map = resourceAndKeys.resource;
              List<Integer> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(recordCount);
                int keyIndex2 =
                    Math.min(
                        keys.size() - 1,
                        keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                int key1 = keys.get(keyIndex1);
                int key2 = keys.get(keyIndex2);
                for (Map.Entry<Integer, Integer> ignored : map.subMap(key1, key2).entrySet()) {
                  // Nothing to do.
                }
              }
            };
          }
        });
  }
}
