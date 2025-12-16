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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;
import org.komamitsu.wormhole4j.Wormhole.Validator;

@ParameterizedClass
@ValueSource(ints = {3, 8, 128})
class WormholeForLongKeyTest {
  @Parameter int leafNodeSize;

  class Common {
    boolean isLeafNodeLargeEnough() {
      return leafNodeSize >= 8;
    }
  }

  @Nested
  class Get extends Common {
    @Test
    void withOneLeafNodeWithOneMinimumKeyRecord_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(Long.MIN_VALUE, 100);

      // Act & Assert
      assertThat(wormhole.get(Long.MIN_VALUE)).isEqualTo(100);
      assertThat(wormhole.get(Long.MIN_VALUE + 1)).isNull();
    }

    @Test
    void withOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(10L, 100);

      // Act & Assert
      assertThat(wormhole.get(9L)).isNull();
      assertThat(wormhole.get(10L)).isEqualTo(100);
      assertThat(wormhole.get(11L)).isNull();
    }

    @Test
    void withOneLeafNodeWithOneEmptyRecordWithUpdatedValue_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(10L, 100);
      wormhole.put(10L, 1000);

      // Act & Assert
      assertThat(wormhole.get(9L)).isNull();
      assertThat(wormhole.get(10L)).isEqualTo(1000);
      assertThat(wormhole.get(11L)).isNull();
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(10L, 100);
      wormhole.put(30L, 300);
      wormhole.put(20L, 200);

      // Act & Assert
      assertThat(wormhole.get(9L)).isNull();
      assertThat(wormhole.get(10L)).isEqualTo(100);
      assertThat(wormhole.get(11L)).isNull();
      assertThat(wormhole.get(19L)).isNull();
      assertThat(wormhole.get(20L)).isEqualTo(200);
      assertThat(wormhole.get(21L)).isNull();
      assertThat(wormhole.get(29L)).isNull();
      assertThat(wormhole.get(30L)).isEqualTo(300);
      assertThat(wormhole.get(31L)).isNull();
    }

    @Test
    void withTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(40L, 400);
      wormhole.put(20L, 200);
      wormhole.put(10L, 100);
      wormhole.put(50L, 500);
      wormhole.put(30L, 300);

      // Act & Assert
      assertThat(wormhole.get(9L)).isNull();
      assertThat(wormhole.get(10L)).isEqualTo(100);
      assertThat(wormhole.get(11L)).isNull();
      assertThat(wormhole.get(19L)).isNull();
      assertThat(wormhole.get(20L)).isEqualTo(200);
      assertThat(wormhole.get(21L)).isNull();
      assertThat(wormhole.get(29L)).isNull();
      assertThat(wormhole.get(30L)).isEqualTo(300);
      assertThat(wormhole.get(31L)).isNull();
      assertThat(wormhole.get(39L)).isNull();
      assertThat(wormhole.get(40L)).isEqualTo(400);
      assertThat(wormhole.get(41L)).isNull();
      assertThat(wormhole.get(49L)).isNull();
      assertThat(wormhole.get(50L)).isEqualTo(500);
      assertThat(wormhole.get(51L)).isNull();
    }

    @Test
    @EnabledIf("isLeafNodeLargeEnough")
    void withManyLeafNodes_ShouldReturnThem() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize);
      Validator<Long, Integer> validator = new Validator<>(wormhole);
      int recordCount = 50000;
      Map<Long, Integer> expected = new LinkedHashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        long key = ThreadLocalRandom.current().nextLong();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormhole.put(key, value);
      }
      validator.validate();

      // Act & Assert
      for (Map.Entry<Long, Integer> entry : expected.entrySet()) {
        assertThat(wormhole.get(entry.getKey())).isEqualTo(entry.getValue());
      }
    }
  }

  @Nested
  class Put extends Common {
    @Test
    void whenPutNewKeyValue_ShouldReturnNull() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);

      // Act & Assert
      assertThat(wormhole.put(10L, 100)).isNull();
    }

    @Test
    void whenPutExistingKeyValue_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);

      // Act & Assert
      assertThat(wormhole.put(10L, 100)).isNull();
      assertThat(wormhole.put(10L, 1000)).isEqualTo(100);
    }
  }

  @Nested
  class Scan extends Common {
    @Test
    void withOneLeafNodeWithOneMinimumKeyRecord_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<String> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(Long.MIN_VALUE, "foo");

      // Act & Assert
      KeyValue<Long, String> firstItem = new KeyValue<>(Long.MIN_VALUE, "foo");
      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 2)).containsExactly(firstItem);

      // With exclusive end keys.
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(
            Long.MIN_VALUE, Long.MIN_VALUE, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(Long.MIN_VALUE, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(null, Long.MIN_VALUE, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(null, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(
            Long.MIN_VALUE + 1, Long.MAX_VALUE, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(Long.MIN_VALUE + 1, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      // With inclusive end keys.
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(
            Long.MIN_VALUE, Long.MIN_VALUE, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(Long.MIN_VALUE, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(null, Long.MIN_VALUE, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(null, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(
            Long.MIN_VALUE + 1, Long.MAX_VALUE, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, String>> result = new ArrayList<>();
        wormhole.scan(Long.MIN_VALUE + 1, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
    }

    @Test
    void withOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(10L, 100);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      assertThat(wormhole.scanWithCount(9L, 0)).isEmpty();
      assertThat(wormhole.scanWithCount(9L, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(9L, 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(10L, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(10L, 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(11L, 1)).isEmpty();

      // With exclusive end keys.
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(10L, 10L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(null, 10L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(10L, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(11L, 11L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(null, 10L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(11L, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      // With inclusive end keys.
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(10L, 10L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(null, 10L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(10L, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(11L, 11L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(null, 9L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(11L, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(30L, 300);
      wormhole.put(20L, 200);
      wormhole.put(10L, 100);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      KeyValue<Long, Integer> secondItem = new KeyValue<>(20L, 200);
      KeyValue<Long, Integer> thirdItem = new KeyValue<>(30L, 300);
      assertThat(wormhole.scanWithCount(9L, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(9L, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount(9L, 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(9L, 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(10L, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(10L, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount(10L, 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(10L, 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(11L, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(11L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(11L, 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(19L, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(19L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(19L, 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(20L, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(20L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(20L, 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(21L, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(21L, 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(29L, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(29L, 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(30L, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(30L, 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(31L, 1)).isEmpty();
      assertThat(wormhole.scanWithCount(31L, 2)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(10L, 30L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(10L, 31L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(11L, 30L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(10L, 30L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(9L, 31L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(11L, 29L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem);
      }
    }

    @Test
    void withTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(10L, 100);
      wormhole.put(20L, 200);
      wormhole.put(30L, 300);
      wormhole.put(40L, 400);
      wormhole.put(50L, 500);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      KeyValue<Long, Integer> secondItem = new KeyValue<>(20L, 200);
      KeyValue<Long, Integer> thirdItem = new KeyValue<>(30L, 300);
      KeyValue<Long, Integer> fourthItem = new KeyValue<>(40L, 400);
      KeyValue<Long, Integer> fifthItem = new KeyValue<>(50L, 500);
      assertThat(wormhole.scanWithCount(9L, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(9L, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount(9L, 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(9L, 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(9L, 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(9L, 6))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(10L, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(10L, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount(10L, 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(10L, 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(10L, 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(10L, 6))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(11L, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(11L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(11L, 3)).containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(11L, 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(11L, 5))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(19L, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(19L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(19L, 3)).containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(19L, 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(19L, 5))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(20L, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(20L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(20L, 3)).containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(20L, 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(20L, 5))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(21L, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(21L, 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(21L, 3)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(21L, 4)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(29L, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(29L, 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(29L, 3)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(29L, 4)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(30L, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(30L, 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(30L, 3)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(30L, 4)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(31L, 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount(31L, 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(31L, 3)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(39L, 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount(39L, 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(39L, 3)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(40L, 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount(40L, 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(40L, 3)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(41L, 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(41L, 2)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(49L, 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(49L, 2)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(50L, 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(50L, 2)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(51L, 1)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(10L, 50L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(9L, 51L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(11L, 50L, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem, thirdItem, fourthItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(10L, 50L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(9L, 51L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<Long, Integer>> result = new ArrayList<>();
        wormhole.scan(11L, 49L, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem, thirdItem, fourthItem);
      }
    }

    @Test
    void whenFunctionReturnsFalse_ShouldStopIterate() {
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(30L, 300);
      wormhole.put(20L, 200);
      wormhole.put(10L, 100);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      KeyValue<Long, Integer> secondItem = new KeyValue<>(20L, 200);

      List<KeyValue<Long, Integer>> result = new ArrayList<>();
      wormhole.scan(
          Long.MIN_VALUE,
          null,
          false,
          (k, v) -> {
            result.add(new KeyValue<>(k, v));
            return k != 20L;
          });
      assertThat(result).containsExactly(firstItem, secondItem);
    }

    @Test
    @EnabledIf("isLeafNodeLargeEnough")
    void withManyLeafNodes_ShouldReturnThem() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize);
      Validator<Long, Integer> validator = new Validator<>(wormhole);
      int recordCount = 50000;
      TreeMap<Long, Integer> expected = new TreeMap<>();
      List<Long> keys = new ArrayList<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        long key = ThreadLocalRandom.current().nextLong();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        keys.add(key);
        wormhole.put(key, value);
      }
      validator.validate();
      Collections.sort(keys);

      // Act & Assert
      List<Map.Entry<Long, Integer>> expectedKeyValues = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        int count = ThreadLocalRandom.current().nextInt(10000);
        {
          long key = ThreadLocalRandom.current().nextLong();
          expectedKeyValues.clear();
          for (Map.Entry<Long, Integer> entry : expected.subMap(key, Long.MAX_VALUE).entrySet()) {
            if (expectedKeyValues.size() >= count) {
              break;
            }
            expectedKeyValues.add(entry);
          }

          List<Map.Entry<Long, Integer>> actual =
              wormhole.scanWithCount(key, count).stream()
                  .map(kv -> new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue()))
                  .collect(Collectors.toList());

          assertThat(actual).containsExactlyElementsOf(expectedKeyValues);
        }
        {
          int startIndex = ThreadLocalRandom.current().nextInt(keys.size());
          int endIndex = Math.min(startIndex + count, keys.size() - 1);
          long startKey = keys.get(startIndex);
          if (i % 2 == 0) {
            startKey++;
          }
          long endKey = keys.get(endIndex);
          if (i % 3 == 0) {
            endKey++;
          }
          if (startKey > endKey) {
            endKey = startKey;
          }
          // With exclusive end keys.
          {
            expectedKeyValues.clear();
            expectedKeyValues.addAll(expected.subMap(startKey, endKey).entrySet());

            List<Map.Entry<Long, Integer>> actualKeyValues = new ArrayList<>(count);
            wormhole.scan(
                startKey,
                endKey,
                true,
                (k, v) -> {
                  actualKeyValues.add(new AbstractMap.SimpleEntry<>(k, v));
                  return true;
                });

            assertThat(actualKeyValues).containsExactlyElementsOf(expectedKeyValues);
          }
          // With inclusive end keys.
          {
            expectedKeyValues.clear();
            expectedKeyValues.addAll(expected.subMap(startKey, true, endKey, true).entrySet());

            List<Map.Entry<Long, Integer>> actualKeyValues = new ArrayList<>(count);
            wormhole.scan(
                startKey,
                endKey,
                false,
                (k, v) -> {
                  actualKeyValues.add(new AbstractMap.SimpleEntry<>(k, v));
                  return true;
                });

            assertThat(actualKeyValues).containsExactlyElementsOf(expectedKeyValues);
          }
        }
      }
    }
  }

  @Nested
  class Delete extends Common {
    @Test
    void withOneRecord_GivenSameMinimumKey_ShouldReturnTrueAndDeleteIt() {
      // Arrange
      WormholeForLongKey<String> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(Long.MIN_VALUE, "foo");

      // Act & Assert
      assertThat(wormhole.delete(Long.MIN_VALUE)).isTrue();

      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenSameKey_ShouldReturnTrueAndDeleteIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(10L, 100);

      // Act & Assert
      assertThat(wormhole.delete(10L)).isTrue();
      assertThat(wormhole.get(9L)).isNull();
      assertThat(wormhole.get(10L)).isNull();
      assertThat(wormhole.get(11L)).isNull();

      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenDifferentKey_ShouldReturnFalseAndNotDeleteIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(10L, 100);

      // Act & Assert
      assertThat(wormhole.delete(9L)).isFalse();
      assertThat(wormhole.delete(11L)).isFalse();
      assertThat(wormhole.get(10L)).isEqualTo(100);

      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000))
          .containsExactly(new KeyValue<>(10L, 100));
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldDeleteThem() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(30L, 300);
      wormhole.put(20L, 200);
      wormhole.put(10L, 100);

      // Act & Assert
      assertThat(wormhole.delete(10L)).isTrue();
      assertThat(wormhole.get(10L)).isNull();
      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(2);

      assertThat(wormhole.delete(30L)).isTrue();
      assertThat(wormhole.get(30L)).isNull();
      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(1);

      assertThat(wormhole.delete(20L)).isTrue();
      assertThat(wormhole.get(20L)).isNull();
      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_ShouldDeleteThem() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize, true);
      wormhole.put(50L, 500);
      wormhole.put(40L, 400);
      wormhole.put(30L, 300);
      wormhole.put(20L, 200);
      wormhole.put(10L, 100);

      // Act & Assert
      assertThat(wormhole.delete(10L)).isTrue();
      assertThat(wormhole.get(10L)).isNull();
      assertThat(wormhole.get(20L)).isEqualTo(200);
      assertThat(wormhole.get(30L)).isEqualTo(300);
      assertThat(wormhole.get(40L)).isEqualTo(400);
      assertThat(wormhole.get(50L)).isEqualTo(500);
      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(4);

      assertThat(wormhole.delete(20L)).isTrue();
      assertThat(wormhole.get(10L)).isNull();
      assertThat(wormhole.get(20L)).isNull();
      assertThat(wormhole.get(30L)).isEqualTo(300);
      assertThat(wormhole.get(40L)).isEqualTo(400);
      assertThat(wormhole.get(50L)).isEqualTo(500);
      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(3);

      assertThat(wormhole.delete(30L)).isTrue();
      assertThat(wormhole.get(10L)).isNull();
      assertThat(wormhole.get(20L)).isNull();
      assertThat(wormhole.get(30L)).isNull();
      assertThat(wormhole.get(40L)).isEqualTo(400);
      assertThat(wormhole.get(50L)).isEqualTo(500);
      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(2);

      assertThat(wormhole.delete(40L)).isTrue();
      assertThat(wormhole.get(10L)).isNull();
      assertThat(wormhole.get(20L)).isNull();
      assertThat(wormhole.get(30L)).isNull();
      assertThat(wormhole.get(40L)).isNull();
      assertThat(wormhole.get(50L)).isEqualTo(500);
      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(1);

      assertThat(wormhole.delete(50L)).isTrue();
      assertThat(wormhole.get(10L)).isNull();
      assertThat(wormhole.get(20L)).isNull();
      assertThat(wormhole.get(30L)).isNull();
      assertThat(wormhole.get(40L)).isNull();
      assertThat(wormhole.get(50L)).isNull();
      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(0);
    }

    @Test
    @EnabledIf("isLeafNodeLargeEnough")
    void withManyLeafNodes_ShouldReturnIt() {
      // Arrange
      WormholeForLongKey<Integer> wormhole = new WormholeForLongKey<>(leafNodeSize);
      Validator<Long, Integer> validator = new Validator<>(wormhole);
      int recordCount = 30000;
      Map<Long, Integer> expected = new HashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        long key = ThreadLocalRandom.current().nextLong();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormhole.put(key, value);
      }
      List<Long> expectedKeys = new ArrayList<>(expected.keySet());

      // Act & Assert

      // 100% -> 50%
      for (int i = 0; i < expected.size() - recordCount * 0.5; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        long key = expectedKeys.get(keyIndex);
        assertThat(wormhole.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      for (Map.Entry<Long, Integer> entry : expected.entrySet()) {
        assertThat(wormhole.get(entry.getKey())).isEqualTo(entry.getValue());
      }

      // 50% -> 5%
      for (int i = 0; i < expected.size() - recordCount * 0.05; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        long key = expectedKeys.get(keyIndex);
        assertThat(wormhole.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      Map<Long, Integer> scanned =
          wormhole.scanWithCount(Long.MIN_VALUE, 100000).stream()
              .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

      for (Map.Entry<Long, Integer> entry : expected.entrySet()) {
        long key = entry.getKey();
        Integer actualValue = wormhole.get(key);
        assertThat(actualValue).isEqualTo(entry.getValue());
        assertThat(scanned.get(key)).isEqualTo(entry.getValue());
      }

      // 100 -> 0
      int remainingCount = expected.size();
      for (int i = 0; i < remainingCount; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        long key = expectedKeys.get(keyIndex);
        assertThat(wormhole.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      assertThat(wormhole.scanWithCount(Long.MIN_VALUE, 100000)).isEmpty();
    }
  }
}
