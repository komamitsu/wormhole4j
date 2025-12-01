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

class BenchmarkForLongKey extends Benchmark {
  private long getRandomKey() {
    return ThreadLocalRandom.current().nextLong();
  }

  @Test
  void insertToWormhole() throws Throwable {
    execute(
        new TestCase<List<Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Insert to Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(getRandomKey());
            }
            return keys;
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(List<Long> keys) {
            return () -> {
              WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>();
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
        new TestCase<List<Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Insert to Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(getRandomKey());
            }
            return keys;
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(List<Long> keys) {
            return () -> {
              TreeMap<Long, Integer> map = new TreeMap<>();
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
        new TestCase<List<Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Insert to AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public List<Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
              keys.add(getRandomKey());
            }
            return keys;
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(List<Long> keys) {
            return () -> {
              Object2ObjectSortedMap<Long, Integer> map = new Object2ObjectAVLTreeMap<>();
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
        new TestCase<ResourceAndKeys<WormholeForLongKey<Integer>, Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Get from Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<WormholeForLongKey<Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              wormhole.put(key, i);
            }
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<WormholeForLongKey<Integer>, Long> resourceAndKeys) {
            return () -> {
              WormholeForLongKey<Integer> wormhole = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
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
        new TestCase<ResourceAndKeys<TreeMap<Long, Integer>, Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Get from Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<TreeMap<Long, Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            TreeMap<Long, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<Long, Integer>, Long> resourceAndKeys) {
            return () -> {
              TreeMap<Long, Integer> map = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
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
            ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Get from AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<Long, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<Long, Integer> map = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
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
        new TestCase<ResourceAndKeys<WormholeForLongKey<Integer>, Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Update Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<WormholeForLongKey<Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              wormhole.put(key, i);
            }
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<WormholeForLongKey<Integer>, Long> resourceAndKeys) {
            return () -> {
              WormholeForLongKey<Integer> wormhole = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
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
        new TestCase<ResourceAndKeys<TreeMap<Long, Integer>, Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Update Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<TreeMap<Long, Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            TreeMap<Long, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<Long, Integer>, Long> resourceAndKeys) {
            return () -> {
              TreeMap<Long, Integer> map = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
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
            ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Update AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount * 2;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<Long, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<Long, Integer> map = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
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
        new TestCase<ResourceAndKeys<WormholeForLongKey<Integer>, Long>, RuntimeException>() {
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
          public ResourceAndKeys<WormholeForLongKey<Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              wormhole.put(key, i);
            }
            Collections.shuffle(keys);
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<WormholeForLongKey<Integer>, Long> resourceAndKeys) {
            return () -> {
              WormholeForLongKey<Integer> wormhole = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                long key = keys.get(i);
                wormhole.delete(key);
              }
            };
          }
        });
  }

  @Test
  void deleteFromRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<Long, Integer>, Long>, RuntimeException>() {
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
          public ResourceAndKeys<TreeMap<Long, Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            TreeMap<Long, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            Collections.shuffle(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<Long, Integer>, Long> resourceAndKeys) {
            return () -> {
              TreeMap<Long, Integer> map = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                long key = keys.get(i);
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
            ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long>, RuntimeException>() {
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
          public ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<Long, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            Collections.shuffle(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<Long, Integer> map = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                long key = keys.get(i);
                map.remove(key);
              }
            };
          }
        });
  }

  @Test
  void scanFromWormhole() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<WormholeForLongKey<Integer>, Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Scan from Wormhole (Wormhole4j)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<WormholeForLongKey<Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              wormhole.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(wormhole, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<WormholeForLongKey<Integer>, Long> resourceAndKeys) {
            return () -> {
              WormholeForLongKey<Integer> wormhole = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(keys.size());
                int keyIndex2 =
                    Math.min(
                        keys.size() - 1,
                        keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                long key1 = keys.get(keyIndex1);
                long key2 = keys.get(keyIndex2);
                wormhole.scan(key1, key2, true, kv -> true);
              }
            };
          }
        });
  }

  @Test
  void scanFromRedBlackTreeMap() throws Throwable {
    execute(
        new TestCase<ResourceAndKeys<TreeMap<Long, Integer>, Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Scan from Red-Black tree (TreeMap)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<TreeMap<Long, Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            TreeMap<Long, Integer> map = new TreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<TreeMap<Long, Integer>, Long> resourceAndKeys) {
            return () -> {
              TreeMap<Long, Integer> map = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(keys.size());
                int keyIndex2 =
                    Math.min(
                        keys.size() - 1,
                        keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                long key1 = keys.get(keyIndex1);
                long key2 = keys.get(keyIndex2);
                for (Map.Entry<Long, Integer> ignored : map.subMap(key1, key2).entrySet()) {
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
            ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long>, RuntimeException>() {
          @Override
          public String label() {
            return "[Int key] Scan from AVL tree map (Fastutil)";
          }

          @Override
          public int count() {
            return recordCount;
          }

          @Override
          public ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long> init() {
            List<Long> keys = new ArrayList<>(recordCount);
            Object2ObjectSortedMap<Long, Integer> map = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < recordCount; i++) {
              long key = getRandomKey();
              keys.add(key);
              map.put(key, i);
            }
            Collections.sort(keys);
            return new ResourceAndKeys<>(map, keys);
          }

          @Override
          public ThrowableRunnable<RuntimeException> createTask(
              ResourceAndKeys<Object2ObjectSortedMap<Long, Integer>, Long> resourceAndKeys) {
            return () -> {
              Object2ObjectSortedMap<Long, Integer> map = resourceAndKeys.resource;
              List<Long> keys = resourceAndKeys.keys;
              for (int i = 0; i < count(); i++) {
                int keyIndex1 = ThreadLocalRandom.current().nextInt(keys.size());
                int keyIndex2 =
                    Math.min(
                        keys.size() - 1,
                        keyIndex1 + ThreadLocalRandom.current().nextInt(maxScanSize));
                long key1 = keys.get(keyIndex1);
                long key2 = keys.get(keyIndex2);
                for (Map.Entry<Long, Integer> ignored : map.subMap(key1, key2).entrySet()) {
                  // Nothing to do.
                }
              }
            };
          }
        });
  }
}
