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
class WormholeForIntKeyTest {
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
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(Integer.MIN_VALUE, 100);

      // Act & Assert
      assertThat(wormhole.get(Integer.MIN_VALUE)).isEqualTo(100);
      assertThat(wormhole.get(Integer.MIN_VALUE + 1)).isNull();
    }

    @Test
    void withOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(10, 100);

      // Act & Assert
      assertThat(wormhole.get(9)).isNull();
      assertThat(wormhole.get(10)).isEqualTo(100);
      assertThat(wormhole.get(11)).isNull();
    }

    @Test
    void withOneLeafNodeWithOneEmptyRecordWithUpdatedValue_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(10, 100);
      wormhole.put(10, 1000);

      // Act & Assert
      assertThat(wormhole.get(9)).isNull();
      assertThat(wormhole.get(10)).isEqualTo(1000);
      assertThat(wormhole.get(11)).isNull();
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(10, 100);
      wormhole.put(30, 300);
      wormhole.put(20, 200);

      // Act & Assert
      assertThat(wormhole.get(9)).isNull();
      assertThat(wormhole.get(10)).isEqualTo(100);
      assertThat(wormhole.get(11)).isNull();
      assertThat(wormhole.get(19)).isNull();
      assertThat(wormhole.get(20)).isEqualTo(200);
      assertThat(wormhole.get(21)).isNull();
      assertThat(wormhole.get(29)).isNull();
      assertThat(wormhole.get(30)).isEqualTo(300);
      assertThat(wormhole.get(31)).isNull();
    }

    @Test
    void withTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(40, 400);
      wormhole.put(20, 200);
      wormhole.put(10, 100);
      wormhole.put(50, 500);
      wormhole.put(30, 300);

      // Act & Assert
      assertThat(wormhole.get(9)).isNull();
      assertThat(wormhole.get(10)).isEqualTo(100);
      assertThat(wormhole.get(11)).isNull();
      assertThat(wormhole.get(19)).isNull();
      assertThat(wormhole.get(20)).isEqualTo(200);
      assertThat(wormhole.get(21)).isNull();
      assertThat(wormhole.get(29)).isNull();
      assertThat(wormhole.get(30)).isEqualTo(300);
      assertThat(wormhole.get(31)).isNull();
      assertThat(wormhole.get(39)).isNull();
      assertThat(wormhole.get(40)).isEqualTo(400);
      assertThat(wormhole.get(41)).isNull();
      assertThat(wormhole.get(49)).isNull();
      assertThat(wormhole.get(50)).isEqualTo(500);
      assertThat(wormhole.get(51)).isNull();
    }

    @Test
    @EnabledIf("isLeafNodeLargeEnough")
    void withManyLeafNodes_ShouldReturnThem() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize);
      Validator<Integer, Integer> validator = new Validator<>(wormhole);
      int recordCount = 50000;
      Map<Integer, Integer> expected = new LinkedHashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        int key = ThreadLocalRandom.current().nextInt();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormhole.put(key, value);
      }
      validator.validate();

      // Act & Assert
      for (Map.Entry<Integer, Integer> entry : expected.entrySet()) {
        assertThat(wormhole.get(entry.getKey())).isEqualTo(entry.getValue());
      }
    }
  }

  @Nested
  class Put extends Common {
    @Test
    void whenPutNewKeyValue_ShouldReturnNull() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);

      // Act & Assert
      assertThat(wormhole.put(10, 100)).isNull();
    }

    @Test
    void whenPutExistingKeyValue_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);

      // Act & Assert
      assertThat(wormhole.put(10, 100)).isNull();
      assertThat(wormhole.put(10, 1000)).isEqualTo(100);
    }
  }

  @Nested
  class Scan extends Common {
    @Test
    void withOneLeafNodeWithOneMinimumKeyRecord_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<String> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(Integer.MIN_VALUE, "foo");

      // Act & Assert
      KeyValue<Integer, String> firstItem = new KeyValue<>(Integer.MIN_VALUE, "foo");
      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 2)).containsExactly(firstItem);

      // With exclusive end keys.
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(
            Integer.MIN_VALUE, Integer.MIN_VALUE, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(Integer.MIN_VALUE, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(null, Integer.MIN_VALUE, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(null, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(
            Integer.MIN_VALUE + 1,
            Integer.MAX_VALUE,
            true,
            (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(
            Integer.MIN_VALUE + 1, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      // With inclusive end keys.
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(
            Integer.MIN_VALUE,
            Integer.MIN_VALUE,
            false,
            (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(Integer.MIN_VALUE, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(null, Integer.MIN_VALUE, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(null, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(
            Integer.MIN_VALUE + 1,
            Integer.MAX_VALUE,
            false,
            (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, String>> result = new ArrayList<>();
        wormhole.scan(
            Integer.MIN_VALUE + 1, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
    }

    @Test
    void withOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(10, 100);

      // Act & Assert
      KeyValue<Integer, Integer> firstItem = new KeyValue<>(10, 100);
      assertThat(wormhole.scanWithCount(9, 0)).isEmpty();
      assertThat(wormhole.scanWithCount(9, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(9, 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(10, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(10, 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(11, 1)).isEmpty();

      // With exclusive end keys.
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(10, 10, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(null, 10, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(10, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(11, 11, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(null, 10, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(11, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      // With inclusive end keys.
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(10, 10, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(null, 10, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(10, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(11, 11, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(null, 9, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(11, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(30, 300);
      wormhole.put(20, 200);
      wormhole.put(10, 100);

      // Act & Assert
      KeyValue<Integer, Integer> firstItem = new KeyValue<>(10, 100);
      KeyValue<Integer, Integer> secondItem = new KeyValue<>(20, 200);
      KeyValue<Integer, Integer> thirdItem = new KeyValue<>(30, 300);
      assertThat(wormhole.scanWithCount(9, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(9, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount(9, 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(9, 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(10, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(10, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount(10, 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(10, 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(11, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(11, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(11, 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(19, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(19, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(19, 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(20, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(20, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(20, 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(21, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(21, 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(29, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(29, 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(30, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(30, 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(31, 1)).isEmpty();
      assertThat(wormhole.scanWithCount(31, 2)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(10, 30, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(10, 31, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(11, 30, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(10, 30, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(9, 31, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(11, 29, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem);
      }
      // With a function that stops the iteration when it retrieves key:20.
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(
            5,
            35,
            false,
            (k, v) -> {
              result.add(new KeyValue<>(k, v));
              return k != 20;
            });
        assertThat(result).containsExactly(firstItem, secondItem);
      }
    }

    @Test
    void withTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(10, 100);
      wormhole.put(20, 200);
      wormhole.put(30, 300);
      wormhole.put(40, 400);
      wormhole.put(50, 500);

      // Act & Assert
      KeyValue<Integer, Integer> firstItem = new KeyValue<>(10, 100);
      KeyValue<Integer, Integer> secondItem = new KeyValue<>(20, 200);
      KeyValue<Integer, Integer> thirdItem = new KeyValue<>(30, 300);
      KeyValue<Integer, Integer> fourthItem = new KeyValue<>(40, 400);
      KeyValue<Integer, Integer> fifthItem = new KeyValue<>(50, 500);
      assertThat(wormhole.scanWithCount(9, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(9, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount(9, 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(9, 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(9, 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(9, 6))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(10, 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount(10, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount(10, 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(10, 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(10, 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(10, 6))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(11, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(11, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(11, 3)).containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(11, 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(11, 5))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(19, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(19, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(19, 3)).containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(19, 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(19, 5))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(20, 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount(20, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount(20, 3)).containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(20, 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(20, 5))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(21, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(21, 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(21, 3)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(21, 4)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(29, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(29, 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(29, 3)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(29, 4)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(30, 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount(30, 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount(30, 3)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(30, 4)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(31, 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount(31, 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(31, 3)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(39, 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount(39, 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(39, 3)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(40, 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount(40, 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(40, 3)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount(41, 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(41, 2)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(49, 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(49, 2)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(50, 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(50, 2)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount(51, 1)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(10, 50, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(9, 51, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(11, 50, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem, thirdItem, fourthItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(10, 50, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(9, 51, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<Integer, Integer>> result = new ArrayList<>();
        wormhole.scan(11, 49, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem, thirdItem, fourthItem);
      }
    }

    @Test
    @EnabledIf("isLeafNodeLargeEnough")
    void withManyLeafNodes_ShouldReturnThem() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize);
      Validator<Integer, Integer> validator = new Validator<>(wormhole);
      int recordCount = 50000;
      TreeMap<Integer, Integer> expected = new TreeMap<>();
      List<Integer> keys = new ArrayList<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        int key = ThreadLocalRandom.current().nextInt();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        keys.add(key);
        wormhole.put(key, value);
      }
      validator.validate();
      Collections.sort(keys);

      // Act & Assert
      List<Map.Entry<Integer, Integer>> expectedKeyValues = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        int count = ThreadLocalRandom.current().nextInt(10000);
        {
          int key = ThreadLocalRandom.current().nextInt();
          expectedKeyValues.clear();
          for (Map.Entry<Integer, Integer> entry :
              expected.subMap(key, Integer.MAX_VALUE).entrySet()) {
            if (expectedKeyValues.size() >= count) {
              break;
            }
            expectedKeyValues.add(entry);
          }

          List<Map.Entry<Integer, Integer>> actual =
              wormhole.scanWithCount(key, count).stream()
                  .map(kv -> new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue()))
                  .collect(Collectors.toList());

          assertThat(actual).containsExactlyElementsOf(expectedKeyValues);
        }
        {
          int startIndex = ThreadLocalRandom.current().nextInt(keys.size());
          int endIndex = Math.min(startIndex + count, keys.size() - 1);
          int startKey = keys.get(startIndex);
          if (i % 2 == 0) {
            startKey++;
          }
          int endKey = keys.get(endIndex);
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

            List<Map.Entry<Integer, Integer>> actualKeyValues = new ArrayList<>(count);
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

            List<Map.Entry<Integer, Integer>> actualKeyValues = new ArrayList<>(count);
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
      WormholeForIntKey<String> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(Integer.MIN_VALUE, "foo");

      // Act & Assert
      assertThat(wormhole.delete(Integer.MIN_VALUE)).isTrue();

      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenSameKey_ShouldReturnTrueAndDeleteIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(10, 100);

      // Act & Assert
      assertThat(wormhole.delete(10)).isTrue();
      assertThat(wormhole.get(9)).isNull();
      assertThat(wormhole.get(10)).isNull();
      assertThat(wormhole.get(11)).isNull();

      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenDifferentKey_ShouldReturnFalseAndNotDeleteIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(10, 100);

      // Act & Assert
      assertThat(wormhole.delete(9)).isFalse();
      assertThat(wormhole.delete(11)).isFalse();
      assertThat(wormhole.get(10)).isEqualTo(100);

      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000))
          .containsExactly(new KeyValue<>(10, 100));
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldDeleteThem() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(30, 300);
      wormhole.put(20, 200);
      wormhole.put(10, 100);

      // Act & Assert
      assertThat(wormhole.delete(10)).isTrue();
      assertThat(wormhole.get(10)).isNull();
      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).size().isEqualTo(2);

      assertThat(wormhole.delete(30)).isTrue();
      assertThat(wormhole.get(30)).isNull();
      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).size().isEqualTo(1);

      assertThat(wormhole.delete(20)).isTrue();
      assertThat(wormhole.get(20)).isNull();
      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_ShouldDeleteThem() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize, true);
      wormhole.put(50, 500);
      wormhole.put(40, 400);
      wormhole.put(30, 300);
      wormhole.put(20, 200);
      wormhole.put(10, 100);

      // Act & Assert
      assertThat(wormhole.delete(10)).isTrue();
      assertThat(wormhole.get(10)).isNull();
      assertThat(wormhole.get(20)).isEqualTo(200);
      assertThat(wormhole.get(30)).isEqualTo(300);
      assertThat(wormhole.get(40)).isEqualTo(400);
      assertThat(wormhole.get(50)).isEqualTo(500);
      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).size().isEqualTo(4);

      assertThat(wormhole.delete(20)).isTrue();
      assertThat(wormhole.get(10)).isNull();
      assertThat(wormhole.get(20)).isNull();
      assertThat(wormhole.get(30)).isEqualTo(300);
      assertThat(wormhole.get(40)).isEqualTo(400);
      assertThat(wormhole.get(50)).isEqualTo(500);
      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).size().isEqualTo(3);

      assertThat(wormhole.delete(30)).isTrue();
      assertThat(wormhole.get(10)).isNull();
      assertThat(wormhole.get(20)).isNull();
      assertThat(wormhole.get(30)).isNull();
      assertThat(wormhole.get(40)).isEqualTo(400);
      assertThat(wormhole.get(50)).isEqualTo(500);
      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).size().isEqualTo(2);

      assertThat(wormhole.delete(40)).isTrue();
      assertThat(wormhole.get(10)).isNull();
      assertThat(wormhole.get(20)).isNull();
      assertThat(wormhole.get(30)).isNull();
      assertThat(wormhole.get(40)).isNull();
      assertThat(wormhole.get(50)).isEqualTo(500);
      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).size().isEqualTo(1);

      assertThat(wormhole.delete(50)).isTrue();
      assertThat(wormhole.get(10)).isNull();
      assertThat(wormhole.get(20)).isNull();
      assertThat(wormhole.get(30)).isNull();
      assertThat(wormhole.get(40)).isNull();
      assertThat(wormhole.get(50)).isNull();
      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).size().isEqualTo(0);
    }

    @Test
    @EnabledIf("isLeafNodeLargeEnough")
    void withManyLeafNodes_ShouldReturnIt() {
      // Arrange
      WormholeForIntKey<Integer> wormhole = new WormholeForIntKey<>(leafNodeSize);
      Validator<Integer, Integer> validator = new Validator<>(wormhole);
      int recordCount = 30000;
      Map<Integer, Integer> expected = new HashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        int key = ThreadLocalRandom.current().nextInt();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormhole.put(key, value);
      }
      List<Integer> expectedKeys = new ArrayList<>(expected.keySet());

      // Act & Assert

      // 100% -> 50%
      for (int i = 0; i < expected.size() - recordCount * 0.5; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        int key = expectedKeys.get(keyIndex);
        assertThat(wormhole.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      for (Map.Entry<Integer, Integer> entry : expected.entrySet()) {
        assertThat(wormhole.get(entry.getKey())).isEqualTo(entry.getValue());
      }

      // 50% -> 5%
      for (int i = 0; i < expected.size() - recordCount * 0.05; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        int key = expectedKeys.get(keyIndex);
        assertThat(wormhole.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      Map<Integer, Integer> scanned =
          wormhole.scanWithCount(Integer.MIN_VALUE, 100000).stream()
              .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

      for (Map.Entry<Integer, Integer> entry : expected.entrySet()) {
        int key = entry.getKey();
        Integer actualValue = wormhole.get(key);
        assertThat(actualValue).isEqualTo(entry.getValue());
        assertThat(scanned.get(key)).isEqualTo(entry.getValue());
      }

      // 100 -> 0
      int remainingCount = expected.size();
      for (int i = 0; i < remainingCount; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        int key = expectedKeys.get(keyIndex);
        assertThat(wormhole.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      assertThat(wormhole.scanWithCount(Integer.MIN_VALUE, 100000)).isEmpty();
    }
  }
}
