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

package org.komamitsu.wormhole4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.komamitsu.wormhole4j.TestHelpers.scan;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

@ParameterizedClass
@ValueSource(ints = {3, 8, 128})
abstract class WormholeForLongKeyTest {
  @Parameter int leafNodeSize;

  class Common {
    boolean isLeafNodeLargeEnough() {
      return leafNodeSize >= 8;
    }
  }

  protected Wormhole<Long, Integer> wormholeForIntValue;
  protected Wormhole<Long, String> wormholeForStrValue;

  @Test
  void givenConcurrentAndDebugModeEnabled_ShouldThrowException() {
    assertThatThrownBy(
            () -> {
              new WormholeBuilder.ForLongKey<Integer>()
                  .setConcurrent(true)
                  .setDebugMode(true)
                  .build();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Nested
  class Get extends Common {
    @Test
    void withOneLeafNodeWithOneMinimumKeyRecord_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put(Long.MIN_VALUE, 100);

      // Act & Assert
      assertThat(wormholeForIntValue.get(Long.MIN_VALUE)).isEqualTo(100);
      assertThat(wormholeForIntValue.get(Long.MIN_VALUE + 1)).isNull();
    }

    @Test
    void withOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      assertThat(wormholeForIntValue.get(9L)).isNull();
      assertThat(wormholeForIntValue.get(10L)).isEqualTo(100);
      assertThat(wormholeForIntValue.get(11L)).isNull();
    }

    @Test
    void withOneLeafNodeWithOneEmptyRecordWithUpdatedValue_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put(10L, 100);
      wormholeForIntValue.put(10L, 1000);

      // Act & Assert
      assertThat(wormholeForIntValue.get(9L)).isNull();
      assertThat(wormholeForIntValue.get(10L)).isEqualTo(1000);
      assertThat(wormholeForIntValue.get(11L)).isNull();
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put(10L, 100);
      wormholeForIntValue.put(30L, 300);
      wormholeForIntValue.put(20L, 200);

      // Act & Assert
      assertThat(wormholeForIntValue.get(9L)).isNull();
      assertThat(wormholeForIntValue.get(10L)).isEqualTo(100);
      assertThat(wormholeForIntValue.get(11L)).isNull();
      assertThat(wormholeForIntValue.get(19L)).isNull();
      assertThat(wormholeForIntValue.get(20L)).isEqualTo(200);
      assertThat(wormholeForIntValue.get(21L)).isNull();
      assertThat(wormholeForIntValue.get(29L)).isNull();
      assertThat(wormholeForIntValue.get(30L)).isEqualTo(300);
      assertThat(wormholeForIntValue.get(31L)).isNull();
    }

    @Test
    void withTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put(40L, 400);
      wormholeForIntValue.put(20L, 200);
      wormholeForIntValue.put(10L, 100);
      wormholeForIntValue.put(50L, 500);
      wormholeForIntValue.put(30L, 300);

      // Act & Assert
      assertThat(wormholeForIntValue.get(9L)).isNull();
      assertThat(wormholeForIntValue.get(10L)).isEqualTo(100);
      assertThat(wormholeForIntValue.get(11L)).isNull();
      assertThat(wormholeForIntValue.get(19L)).isNull();
      assertThat(wormholeForIntValue.get(20L)).isEqualTo(200);
      assertThat(wormholeForIntValue.get(21L)).isNull();
      assertThat(wormholeForIntValue.get(29L)).isNull();
      assertThat(wormholeForIntValue.get(30L)).isEqualTo(300);
      assertThat(wormholeForIntValue.get(31L)).isNull();
      assertThat(wormholeForIntValue.get(39L)).isNull();
      assertThat(wormholeForIntValue.get(40L)).isEqualTo(400);
      assertThat(wormholeForIntValue.get(41L)).isNull();
      assertThat(wormholeForIntValue.get(49L)).isNull();
      assertThat(wormholeForIntValue.get(50L)).isEqualTo(500);
      assertThat(wormholeForIntValue.get(51L)).isNull();
    }

    @Test
    @EnabledIf("isLeafNodeLargeEnough")
    void withManyLeafNodes_ShouldReturnThem() {
      // Arrange
      WormholeValidator<Long, Integer> validator = new WormholeValidator<>(wormholeForIntValue);
      int recordCount = 30000;
      Map<Long, Integer> expected = new LinkedHashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        long key = ThreadLocalRandom.current().nextLong();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormholeForIntValue.put(key, value);
      }
      validator.validate();

      // Act & Assert
      for (Map.Entry<Long, Integer> entry : expected.entrySet()) {
        assertThat(wormholeForIntValue.get(entry.getKey())).isEqualTo(entry.getValue());
      }
    }
  }

  @Nested
  class Put extends Common {
    @Test
    void whenPutNewKeyValue_ShouldReturnNull() {
      // Arrange

      // Act & Assert
      assertThat(wormholeForIntValue.put(10L, 100)).isNull();
    }

    @Test
    void whenPutExistingKeyValue_ShouldReturnIt() {
      // Arrange

      // Act & Assert
      assertThat(wormholeForIntValue.put(10L, 100)).isNull();
      assertThat(wormholeForIntValue.put(10L, 1000)).isEqualTo(100);
    }
  }

  @Nested
  class Scan extends Common {
    @Test
    void scanWithCount_WithOneLeafNodeWithOneMinimumKeyRecord_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put(Long.MIN_VALUE, "foo");

      // Act & Assert
      KeyValue<Long, String> firstItem = new KeyValue<>(Long.MIN_VALUE, "foo");
      assertThat(wormholeForStrValue.scanWithCount(Long.MIN_VALUE, 2)).containsExactly(firstItem);
    }

    @ParameterizedTest
    @EnumSource(TestHelpers.ScanType.class)
    void scan_WithOneLeafNodeWithOneMinimumKeyRecord_ShouldReturnIt(TestHelpers.ScanType scanType) {
      // Arrange
      wormholeForStrValue.put(Long.MIN_VALUE, "foo");

      // Act & Assert
      KeyValue<Long, String> firstItem = new KeyValue<>(Long.MIN_VALUE, "foo");
      // With exclusive end keys.
      assertThat(scan(wormholeForStrValue, scanType, Long.MIN_VALUE, Long.MIN_VALUE, true))
          .isEmpty();
      assertThat(scan(wormholeForStrValue, scanType, Long.MIN_VALUE, null, true))
          .containsExactly(firstItem);
      assertThat(scan(wormholeForStrValue, scanType, null, Long.MIN_VALUE, true)).isEmpty();
      assertThat(scan(wormholeForStrValue, scanType, null, null, true)).containsExactly(firstItem);
      assertThat(scan(wormholeForStrValue, scanType, Long.MIN_VALUE + 1, Long.MAX_VALUE, true))
          .isEmpty();
      assertThat(scan(wormholeForStrValue, scanType, Long.MIN_VALUE + 1, null, true)).isEmpty();
      // With inclusive end keys.
      assertThat(scan(wormholeForStrValue, scanType, Long.MIN_VALUE, Long.MIN_VALUE, false))
          .containsExactly(firstItem);
      assertThat(scan(wormholeForStrValue, scanType, Long.MIN_VALUE, null, false))
          .containsExactly(firstItem);
      assertThat(scan(wormholeForStrValue, scanType, null, Long.MIN_VALUE, false))
          .containsExactly(firstItem);
      assertThat(scan(wormholeForStrValue, scanType, null, null, false)).containsExactly(firstItem);
      assertThat(scan(wormholeForStrValue, scanType, Long.MIN_VALUE + 1, Long.MAX_VALUE, false))
          .isEmpty();
      assertThat(scan(wormholeForStrValue, scanType, Long.MIN_VALUE + 1, null, false)).isEmpty();
    }

    @Test
    void scanWithCount_WithOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      assertThat(wormholeForIntValue.scanWithCount(9L, 0)).isEmpty();
      assertThat(wormholeForIntValue.scanWithCount(9L, 1)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount(9L, 2)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 1)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 2)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount(11L, 1)).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(TestHelpers.ScanType.class)
    void scan_WithOneLeafNodeWithOneRecord_ShouldReturnIt(TestHelpers.ScanType scanType) {
      // Arrange
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      // With exclusive end keys.
      assertThat(scan(wormholeForIntValue, scanType, 10L, 10L, true)).isEmpty();
      assertThat(scan(wormholeForIntValue, scanType, null, 10L, true)).isEmpty();
      assertThat(scan(wormholeForIntValue, scanType, 10L, null, true)).containsExactly(firstItem);
      assertThat(scan(wormholeForIntValue, scanType, 11L, 11L, true)).isEmpty();
      assertThat(scan(wormholeForIntValue, scanType, null, 10L, true)).isEmpty();
      assertThat(scan(wormholeForIntValue, scanType, 11L, null, true)).isEmpty();
      // With inclusive end keys.
      assertThat(scan(wormholeForIntValue, scanType, 10L, 10L, false)).containsExactly(firstItem);
      assertThat(scan(wormholeForIntValue, scanType, null, 10L, false)).containsExactly(firstItem);
      assertThat(scan(wormholeForIntValue, scanType, 10L, null, false)).containsExactly(firstItem);
      assertThat(scan(wormholeForIntValue, scanType, 11L, 11L, false)).isEmpty();
      assertThat(scan(wormholeForIntValue, scanType, null, 9L, false)).isEmpty();
      assertThat(scan(wormholeForIntValue, scanType, 11L, null, false)).isEmpty();
    }

    @Test
    void scanWithCount_WithOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put(30L, 300);
      wormholeForIntValue.put(20L, 200);
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      KeyValue<Long, Integer> secondItem = new KeyValue<>(20L, 200);
      KeyValue<Long, Integer> thirdItem = new KeyValue<>(30L, 300);
      assertThat(wormholeForIntValue.scanWithCount(9L, 1)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount(9L, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForIntValue.scanWithCount(9L, 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(9L, 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 1)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(11L, 1)).containsExactly(secondItem);
      assertThat(wormholeForIntValue.scanWithCount(11L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(11L, 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(19L, 1)).containsExactly(secondItem);
      assertThat(wormholeForIntValue.scanWithCount(19L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(19L, 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(20L, 1)).containsExactly(secondItem);
      assertThat(wormholeForIntValue.scanWithCount(20L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(20L, 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(21L, 1)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(21L, 2)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(29L, 1)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(29L, 2)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(30L, 1)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(30L, 2)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(31L, 1)).isEmpty();
      assertThat(wormholeForIntValue.scanWithCount(31L, 2)).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(TestHelpers.ScanType.class)
    void scan_WithOneLeafNodeWithMaxRecords_ShouldReturnIt(TestHelpers.ScanType scanType) {
      // Arrange
      wormholeForIntValue.put(30L, 300);
      wormholeForIntValue.put(20L, 200);
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      KeyValue<Long, Integer> secondItem = new KeyValue<>(20L, 200);
      KeyValue<Long, Integer> thirdItem = new KeyValue<>(30L, 300);
      // With exclusive end keys.
      assertThat(scan(wormholeForIntValue, scanType, 10L, 30L, true))
          .containsExactly(firstItem, secondItem);
      assertThat(scan(wormholeForIntValue, scanType, 10L, 31L, true))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(scan(wormholeForIntValue, scanType, 11L, 30L, true)).containsExactly(secondItem);
      // With inclusive end keys.
      assertThat(scan(wormholeForIntValue, scanType, 10L, 30L, false))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(scan(wormholeForIntValue, scanType, 9L, 31L, false))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(scan(wormholeForIntValue, scanType, 11L, 29L, false)).containsExactly(secondItem);
    }

    @Test
    void scanWithCount_WithTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put(10L, 100);
      wormholeForIntValue.put(20L, 200);
      wormholeForIntValue.put(30L, 300);
      wormholeForIntValue.put(40L, 400);
      wormholeForIntValue.put(50L, 500);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      KeyValue<Long, Integer> secondItem = new KeyValue<>(20L, 200);
      KeyValue<Long, Integer> thirdItem = new KeyValue<>(30L, 300);
      KeyValue<Long, Integer> fourthItem = new KeyValue<>(40L, 400);
      KeyValue<Long, Integer> fifthItem = new KeyValue<>(50L, 500);
      assertThat(wormholeForIntValue.scanWithCount(9L, 1)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount(9L, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForIntValue.scanWithCount(9L, 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(9L, 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(9L, 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(9L, 6))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 1)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(10L, 6))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(11L, 1)).containsExactly(secondItem);
      assertThat(wormholeForIntValue.scanWithCount(11L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(11L, 3))
          .containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(11L, 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(11L, 5))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(19L, 1)).containsExactly(secondItem);
      assertThat(wormholeForIntValue.scanWithCount(19L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(19L, 3))
          .containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(19L, 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(19L, 5))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(20L, 1)).containsExactly(secondItem);
      assertThat(wormholeForIntValue.scanWithCount(20L, 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(20L, 3))
          .containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(20L, 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(20L, 5))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(21L, 1)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(21L, 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(21L, 3))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(21L, 4))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(29L, 1)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(29L, 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(29L, 3))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(29L, 4))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(30L, 1)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount(30L, 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(30L, 3))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(30L, 4))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(31L, 1)).containsExactly(fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(31L, 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(31L, 3)).containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(39L, 1)).containsExactly(fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(39L, 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(39L, 3)).containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(40L, 1)).containsExactly(fourthItem);
      assertThat(wormholeForIntValue.scanWithCount(40L, 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(40L, 3)).containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(41L, 1)).containsExactly(fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(41L, 2)).containsExactly(fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(49L, 1)).containsExactly(fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(49L, 2)).containsExactly(fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(50L, 1)).containsExactly(fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(50L, 2)).containsExactly(fifthItem);
      assertThat(wormholeForIntValue.scanWithCount(51L, 1)).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(TestHelpers.ScanType.class)
    void scan_WithTwoLeafNodes_ShouldReturnIt(TestHelpers.ScanType scanType) {
      // Arrange
      wormholeForIntValue.put(10L, 100);
      wormholeForIntValue.put(20L, 200);
      wormholeForIntValue.put(30L, 300);
      wormholeForIntValue.put(40L, 400);
      wormholeForIntValue.put(50L, 500);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      KeyValue<Long, Integer> secondItem = new KeyValue<>(20L, 200);
      KeyValue<Long, Integer> thirdItem = new KeyValue<>(30L, 300);
      KeyValue<Long, Integer> fourthItem = new KeyValue<>(40L, 400);
      KeyValue<Long, Integer> fifthItem = new KeyValue<>(50L, 500);
      // With exclusive end keys.
      assertThat(scan(wormholeForIntValue, scanType, 10L, 50L, true))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(scan(wormholeForIntValue, scanType, 9L, 51L, true))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(scan(wormholeForIntValue, scanType, 11L, 50L, true))
          .containsExactly(secondItem, thirdItem, fourthItem);
      // With inclusive end keys.
      assertThat(scan(wormholeForIntValue, scanType, 10L, 50L, false))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(scan(wormholeForIntValue, scanType, 9L, 51L, false))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(scan(wormholeForIntValue, scanType, 11L, 49L, false))
          .containsExactly(secondItem, thirdItem, fourthItem);
    }

    @Test
    void whenFunctionReturnsFalse_ShouldStopIterate() {
      wormholeForIntValue.put(30L, 300);
      wormholeForIntValue.put(20L, 200);
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      KeyValue<Long, Integer> firstItem = new KeyValue<>(10L, 100);
      KeyValue<Long, Integer> secondItem = new KeyValue<>(20L, 200);

      List<KeyValue<Long, Integer>> result = new ArrayList<>();
      wormholeForIntValue.scan(
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
      WormholeValidator<Long, Integer> validator = new WormholeValidator<>(wormholeForIntValue);
      int recordCount = 30000;
      TreeMap<Long, Integer> expected = new TreeMap<>();
      List<Long> keys = new ArrayList<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        long key = ThreadLocalRandom.current().nextLong();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        keys.add(key);
        wormholeForIntValue.put(key, value);
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
              wormholeForIntValue.scanWithCount(key, count).stream()
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
            wormholeForIntValue.scan(
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
            wormholeForIntValue.scan(
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
      wormholeForStrValue.put(Long.MIN_VALUE, "foo");

      // Act & Assert
      assertThat(wormholeForStrValue.delete(Long.MIN_VALUE)).isEqualTo("foo");

      assertThat(wormholeForStrValue.scanWithCount(Long.MIN_VALUE, 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenSameKey_ShouldReturnTrueAndDeleteIt() {
      // Arrange
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      assertThat(wormholeForIntValue.delete(10L)).isEqualTo(100);
      assertThat(wormholeForIntValue.get(9L)).isNull();
      assertThat(wormholeForIntValue.get(10L)).isNull();
      assertThat(wormholeForIntValue.get(11L)).isNull();

      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenDifferentKey_ShouldReturnFalseAndNotDeleteIt() {
      // Arrange
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      assertThat(wormholeForIntValue.delete(9L)).isNull();
      assertThat(wormholeForIntValue.delete(11L)).isNull();
      assertThat(wormholeForIntValue.get(10L)).isEqualTo(100);

      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000))
          .containsExactly(new KeyValue<>(10L, 100));
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldDeleteThem() {
      // Arrange
      wormholeForIntValue.put(30L, 300);
      wormholeForIntValue.put(20L, 200);
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      assertThat(wormholeForIntValue.delete(10L)).isEqualTo(100);
      assertThat(wormholeForIntValue.get(10L)).isNull();
      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(2);

      assertThat(wormholeForIntValue.delete(30L)).isEqualTo(300);
      assertThat(wormholeForIntValue.get(30L)).isNull();
      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(1);

      assertThat(wormholeForIntValue.delete(20L)).isEqualTo(200);
      assertThat(wormholeForIntValue.get(20L)).isNull();
      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_ShouldDeleteThem() {
      // Arrange
      wormholeForIntValue.put(50L, 500);
      wormholeForIntValue.put(40L, 400);
      wormholeForIntValue.put(30L, 300);
      wormholeForIntValue.put(20L, 200);
      wormholeForIntValue.put(10L, 100);

      // Act & Assert
      assertThat(wormholeForIntValue.delete(10L)).isEqualTo(100);
      assertThat(wormholeForIntValue.get(10L)).isNull();
      assertThat(wormholeForIntValue.get(20L)).isEqualTo(200);
      assertThat(wormholeForIntValue.get(30L)).isEqualTo(300);
      assertThat(wormholeForIntValue.get(40L)).isEqualTo(400);
      assertThat(wormholeForIntValue.get(50L)).isEqualTo(500);
      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(4);

      assertThat(wormholeForIntValue.delete(20L)).isEqualTo(200);
      assertThat(wormholeForIntValue.get(10L)).isNull();
      assertThat(wormholeForIntValue.get(20L)).isNull();
      assertThat(wormholeForIntValue.get(30L)).isEqualTo(300);
      assertThat(wormholeForIntValue.get(40L)).isEqualTo(400);
      assertThat(wormholeForIntValue.get(50L)).isEqualTo(500);
      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(3);

      assertThat(wormholeForIntValue.delete(30L)).isEqualTo(300);
      assertThat(wormholeForIntValue.get(10L)).isNull();
      assertThat(wormholeForIntValue.get(20L)).isNull();
      assertThat(wormholeForIntValue.get(30L)).isNull();
      assertThat(wormholeForIntValue.get(40L)).isEqualTo(400);
      assertThat(wormholeForIntValue.get(50L)).isEqualTo(500);
      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(2);

      assertThat(wormholeForIntValue.delete(40L)).isEqualTo(400);
      assertThat(wormholeForIntValue.get(10L)).isNull();
      assertThat(wormholeForIntValue.get(20L)).isNull();
      assertThat(wormholeForIntValue.get(30L)).isNull();
      assertThat(wormholeForIntValue.get(40L)).isNull();
      assertThat(wormholeForIntValue.get(50L)).isEqualTo(500);
      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(1);

      assertThat(wormholeForIntValue.delete(50L)).isEqualTo(500);
      assertThat(wormholeForIntValue.get(10L)).isNull();
      assertThat(wormholeForIntValue.get(20L)).isNull();
      assertThat(wormholeForIntValue.get(30L)).isNull();
      assertThat(wormholeForIntValue.get(40L)).isNull();
      assertThat(wormholeForIntValue.get(50L)).isNull();
      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).size().isEqualTo(0);
    }

    @Test
    @EnabledIf("isLeafNodeLargeEnough")
    void withManyLeafNodes_ShouldReturnIt() {
      // Arrange
      WormholeValidator<Long, Integer> validator = new WormholeValidator<>(wormholeForIntValue);
      int recordCount = 30000;
      Map<Long, Integer> expected = new HashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        long key = ThreadLocalRandom.current().nextLong();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormholeForIntValue.put(key, value);
      }
      List<Long> expectedKeys = new ArrayList<>(expected.keySet());

      // Act & Assert

      // 100% -> 50%
      for (int i = 0; i < expected.size() - recordCount * 0.5; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        long key = expectedKeys.get(keyIndex);
        int value = expected.get(key);
        assertThat(wormholeForIntValue.delete(key)).isEqualTo(value);
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      for (Map.Entry<Long, Integer> entry : expected.entrySet()) {
        assertThat(wormholeForIntValue.get(entry.getKey())).isEqualTo(entry.getValue());
      }

      // 50% -> 5%
      for (int i = 0; i < expected.size() - recordCount * 0.05; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        long key = expectedKeys.get(keyIndex);
        int value = expected.get(key);
        assertThat(wormholeForIntValue.delete(key)).isEqualTo(value);
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      Map<Long, Integer> scanned =
          wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000).stream()
              .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

      for (Map.Entry<Long, Integer> entry : expected.entrySet()) {
        long key = entry.getKey();
        Integer actualValue = wormholeForIntValue.get(key);
        assertThat(actualValue).isEqualTo(entry.getValue());
        assertThat(scanned.get(key)).isEqualTo(entry.getValue());
      }

      // 100 -> 0
      int remainingCount = expected.size();
      for (int i = 0; i < remainingCount; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        long key = expectedKeys.get(keyIndex);
        int value = expected.get(key);
        assertThat(wormholeForIntValue.delete(key)).isEqualTo(value);
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      assertThat(wormholeForIntValue.scanWithCount(Long.MIN_VALUE, 100000)).isEmpty();
    }
  }
}
