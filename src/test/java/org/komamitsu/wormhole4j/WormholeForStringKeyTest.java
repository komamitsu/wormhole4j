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
import static org.komamitsu.wormhole4j.TestHelpers.genRandomKey;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;
import org.komamitsu.wormhole4j.Wormhole.Validator;

@ParameterizedClass
@ValueSource(ints = {3, 8, 128})
class WormholeForStringKeyTest {
  @Parameter int leafNodeSize;

  class Common {
    boolean isLeafNodeLargeEnough() {
      return leafNodeSize >= 8;
    }
  }

  private WormholeForStringKey<Integer> wormholeForIntValue;
  private WormholeForStringKey<String> wormholeForStrValue;

  @BeforeEach
  void setUp() {
    wormholeForIntValue = new WormholeForStringKey.Builder<Integer>()
        .setLeafNodeSize(leafNodeSize)
        .setDebugMode(true)
        .build();

    wormholeForStrValue = new WormholeForStringKey.Builder<String>()
        .setLeafNodeSize(leafNodeSize)
        .setDebugMode(true)
        .build();
  }

  @Nested
  class Get extends Common {
    @Test
    void withOneLeafNodeWithOneEmptyRecord_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put("", "foo");

      // Act & Assert
      assertThat(wormholeForStrValue.get("")).isEqualTo("foo");
    }

    @Test
    void withOneLeafNodeWithOneEmptyRecordWithUpdatedValue_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put("", "foo");
      wormholeForStrValue.put("", "bar");

      // Act & Assert
      assertThat(wormholeForStrValue.get("")).isEqualTo("bar");
    }

    @Test
    void withOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");

      // Act & Assert
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");
      assertThat(wormholeForStrValue.get("Jame")).isNull();
      assertThat(wormholeForStrValue.get("Jamez")).isNull();
      assertThat(wormholeForStrValue.get("Jamesa")).isNull();
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");
      wormholeForStrValue.put("John", "nhoj");
      wormholeForStrValue.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");
      assertThat(wormholeForStrValue.get("Jame")).isNull();
      assertThat(wormholeForStrValue.get("Jason")).isEqualTo("nosaj");
      assertThat(wormholeForStrValue.get("Jasona")).isNull();
      assertThat(wormholeForStrValue.get("John")).isEqualTo("nhoj");
      assertThat(wormholeForStrValue.get("Johh")).isNull();
    }

    @Test
    void withTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");
      wormholeForStrValue.put("Joseph", "hpesoj");
      wormholeForStrValue.put("John", "nhoj");
      wormholeForStrValue.put("Jacob", "bocaj");
      wormholeForStrValue.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormholeForStrValue.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormholeForStrValue.get("Jaco")).isNull();
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");
      assertThat(wormholeForStrValue.get("Jame")).isNull();
      assertThat(wormholeForStrValue.get("Jason")).isEqualTo("nosaj");
      assertThat(wormholeForStrValue.get("Jasona")).isNull();
      assertThat(wormholeForStrValue.get("John")).isEqualTo("nhoj");
      assertThat(wormholeForStrValue.get("Johh")).isNull();
      assertThat(wormholeForStrValue.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormholeForStrValue.get("Josepha")).isNull();
    }

    @Test
    void withTwoLeafNodes_UsingSameCharForKey_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put("aaaaa", 5);
      wormholeForIntValue.put("a", 1);
      wormholeForIntValue.put("aaa", 3);
      wormholeForIntValue.put("aaaa", 4);
      wormholeForIntValue.put("aa", 2);

      // Act & Assert
      assertThat(wormholeForIntValue.get("a")).isEqualTo(1);
      assertThat(wormholeForIntValue.get("aa")).isEqualTo(2);
      assertThat(wormholeForIntValue.get("aaa")).isEqualTo(3);
      assertThat(wormholeForIntValue.get("aaaa")).isEqualTo(4);
      assertThat(wormholeForIntValue.get("aaaaa")).isEqualTo(5);
    }

    @EnabledIf("isLeafNodeLargeEnough")
    @Test
    void withManyLeafNodes_ShouldReturnThem() {
      // Arrange
      Validator<String, Integer> validator = new Validator<>(wormholeForIntValue);
      int minKeyLength = 4;
      int maxKeyLength = 16;
      // FIXME
      // int recordCount = 50000;
      int recordCount = 5000;
      Map<String, Integer> expected = new LinkedHashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        String key = genRandomKey(minKeyLength, maxKeyLength);
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormholeForIntValue.put(key, value);
      }
      validator.validate();

      // Act & Assert
      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        assertThat(wormholeForIntValue.get(entry.getKey())).isEqualTo(entry.getValue());
      }
    }
  }

  @Nested
  class Put extends Common {
    @Test
    void whenPutNewKeyValue_ShouldReturnNull() {
      // Act & Assert
      assertThat(wormholeForStrValue.put("James", "semaj")).isNull();
    }

    @Test
    void whenPutExistingKeyValue_ShouldReturnIt() {
      // Act & Assert
      assertThat(wormholeForStrValue.put("James", "semaj")).isNull();
      assertThat(wormholeForStrValue.put("James", "zzzzz")).isEqualTo("semaj");
    }
  }

  @Nested
  class Scan extends Common {
    @Test
    void withOneLeafNodeWithOneEmptyRecord_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put("", "foo");

      // Act & Assert
      KeyValue<String, String> firstItem = new KeyValue<>("", "foo");
      assertThat(wormholeForStrValue.scanWithCount("", 2)).containsExactly(firstItem);

      // With exclusive end keys.
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange("", "", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange("", null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(null, "", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(null, null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange("a", "z", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange("a", null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange("", "", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange("", null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(null, "", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            null, null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange("a", "z", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange("a", null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
    }

    @Test
    void withOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");

      // Act & Assert
      KeyValue<String, String> firstItem = new KeyValue<>("James", "semaj");
      assertThat(wormholeForStrValue.scanWithCount("J", 0)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("J", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 2)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 2)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jam", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jam", 2)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jame", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jame", 2)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("James", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("James", 2)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("I", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("I", 2)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jal", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jal", 2)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamd", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamd", 2)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamer", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamer", 2)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("K", 2)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Jb", 2)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Jan", 2)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Jamf", 2)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Jamet", 2)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "James", "James", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            null, "James", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "James", null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jamesa", "Jamesa", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            null, "Jamer", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jamesa", null, true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "James", "James", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            null, "James", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "James", null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jamesa", "Jamesa", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            null, "Jamer", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jamesa", null, false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).isEmpty();
      }
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");
      wormholeForStrValue.put("John", "nhoj");
      wormholeForStrValue.put("Jason", "nosaj");

      // Act & Assert
      KeyValue<String, String> firstItem = new KeyValue<>("James", "semaj");
      KeyValue<String, String> secondItem = new KeyValue<>("Jason", "nosaj");
      KeyValue<String, String> thirdItem = new KeyValue<>("John", "nhoj");
      assertThat(wormholeForStrValue.scanWithCount("I", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("I", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("I", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("I", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 0)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("J", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jal", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jal", 2))
          .containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jal", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jal", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jam", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jam", 2))
          .containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jam", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jam", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jan", 1)).containsExactly(secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jan", 2))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jan", 3))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamd", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamd", 2))
          .containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamd", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamd", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jame", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jame", 2))
          .containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jame", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jame", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamf", 1)).containsExactly(secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamf", 2))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamf", 3))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamer", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamer", 2))
          .containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamer", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamer", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("James", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("James", 2))
          .containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("James", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("James", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamet", 1)).containsExactly(secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamet", 2))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jamet", 3))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jar", 1)).containsExactly(secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jar", 2))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jar", 3))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jas", 1)).containsExactly(secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jas", 2))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jas", 3))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jat", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jat", 2)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasn", 1)).containsExactly(secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasn", 2))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasn", 3))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jaso", 1)).containsExactly(secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jaso", 2))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jaso", 3))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasp", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasp", 2)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasol", 1)).containsExactly(secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasol", 2))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasol", 3))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jason", 1)).containsExactly(secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Jason", 2))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jason", 3))
          .containsExactly(secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasoo", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasoo", 2)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jn", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jn", 2)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jo", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jo", 2)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jp", 1)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Jog", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jog", 2)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Joh", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Joh", 2)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Joi", 1)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Johm", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Johm", 2)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("John", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("John", 2)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Joho", 1)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("K", 1)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "James", "John", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jamer", "Johna", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jamesa", "Johm", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "James", "John", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jamer", "Johna", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jamesa", "Johm", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem);
      }
    }

    @Test
    void withTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");
      wormholeForStrValue.put("Joseph", "hpesoj");
      wormholeForStrValue.put("John", "nhoj");
      wormholeForStrValue.put("Jacob", "bocaj");
      wormholeForStrValue.put("Jason", "nosaj");

      // Act & Assert
      KeyValue<String, String> firstItem = new KeyValue<>("Jacob", "bocaj");
      KeyValue<String, String> secondItem = new KeyValue<>("James", "semaj");
      KeyValue<String, String> thirdItem = new KeyValue<>("Jason", "nosaj");
      KeyValue<String, String> fourthItem = new KeyValue<>("John", "nhoj");
      KeyValue<String, String> fifthItem = new KeyValue<>("Joseph", "hpesoj");
      assertThat(wormholeForStrValue.scanWithCount("I", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("I", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("I", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("I", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("I", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("J", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 1)).containsExactly(firstItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Ja", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasom", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasom", 2))
          .containsExactly(thirdItem, fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasom", 3))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jason", 1)).containsExactly(thirdItem);
      assertThat(wormholeForStrValue.scanWithCount("Jason", 2))
          .containsExactly(thirdItem, fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jason", 3))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasoo", 1)).containsExactly(fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jasoo", 2))
          .containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jb", 1)).containsExactly(fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jb", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jm", 1)).containsExactly(fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jm", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jo", 1)).containsExactly(fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jo", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jog", 1)).containsExactly(fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jog", 2))
          .containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Joh", 1)).containsExactly(fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Joh", 2))
          .containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Johm", 1)).containsExactly(fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("Johm", 2))
          .containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("John", 1)).containsExactly(fourthItem);
      assertThat(wormholeForStrValue.scanWithCount("John", 2))
          .containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Joho", 1)).containsExactly(fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jor", 1)).containsExactly(fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jos", 1)).containsExactly(fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Josd", 1)).containsExactly(fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Jose", 1)).containsExactly(fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Joseo", 1)).containsExactly(fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Josep", 1)).containsExactly(fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Josepg", 1)).containsExactly(fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Joseph", 1)).containsExactly(fifthItem);
      assertThat(wormholeForStrValue.scanWithCount("Josepha", 1)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Josepi", 1)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Joseq", 1)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Josf", 1)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Jot", 1)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("Jp", 1)).isEmpty();
      assertThat(wormholeForStrValue.scanWithCount("K", 1)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jacob", "Joseph", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jacoa", "Josepha", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jacoba", "Josepg", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem, thirdItem, fourthItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jacob", "Joseph", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jacoa", "Josepha", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<String, String>> result = new ArrayList<>();
        wormholeForStrValue.scanRange(
            "Jacoba", "Josepg", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(secondItem, thirdItem, fourthItem);
      }
    }

    @Test
    void withTwoLeafNodes_UsingSameCharForKey_ShouldReturnIt() {
      // Arrange
      wormholeForIntValue.put("aaaaa", 5);
      wormholeForIntValue.put("a", 1);
      wormholeForIntValue.put("aaa", 3);
      wormholeForIntValue.put("aaaa", 4);
      wormholeForIntValue.put("aa", 2);

      // Act & Assert
      KeyValue<String, Integer> firstItem = new KeyValue<>("a", 1);
      KeyValue<String, Integer> secondItem = new KeyValue<>("aa", 2);
      KeyValue<String, Integer> thirdItem = new KeyValue<>("aaa", 3);
      KeyValue<String, Integer> fourthItem = new KeyValue<>("aaaa", 4);
      KeyValue<String, Integer> fifthItem = new KeyValue<>("aaaaa", 5);

      assertThat(wormholeForIntValue.scanWithCount("", 1)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount("", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForIntValue.scanWithCount("", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount("", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount("", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount("a", 1)).containsExactly(firstItem);
      assertThat(wormholeForIntValue.scanWithCount("a", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormholeForIntValue.scanWithCount("a", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount("a", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount("a", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount("aa", 1)).containsExactly(secondItem);
      assertThat(wormholeForIntValue.scanWithCount("aa", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormholeForIntValue.scanWithCount("aa", 3))
          .containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount("aa", 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount("ab", 1)).isEmpty();
      assertThat(wormholeForIntValue.scanWithCount("aaa", 1)).containsExactly(thirdItem);
      assertThat(wormholeForIntValue.scanWithCount("aaa", 2))
          .containsExactly(thirdItem, fourthItem);
      assertThat(wormholeForIntValue.scanWithCount("aaa", 3))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount("aab", 1)).isEmpty();
      assertThat(wormholeForIntValue.scanWithCount("aaaa", 1)).containsExactly(fourthItem);
      assertThat(wormholeForIntValue.scanWithCount("aaaa", 2))
          .containsExactly(fourthItem, fifthItem);
      assertThat(wormholeForIntValue.scanWithCount("aaab", 1)).isEmpty();
      assertThat(wormholeForIntValue.scanWithCount("aaaaa", 1)).containsExactly(fifthItem);
      assertThat(wormholeForIntValue.scanWithCount("aaaab", 1)).isEmpty();
      assertThat(wormholeForIntValue.scanWithCount("b", 1)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<String, Integer>> result = new ArrayList<>();
        wormholeForIntValue.scanRange(
            "a", "aaaaa", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      }
      {
        List<KeyValue<String, Integer>> result = new ArrayList<>();
        wormholeForIntValue.scanRange(
            "", "aaaab", true, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, Integer>> result = new ArrayList<>();
        wormholeForIntValue.scanRange(
            "a", "aaaaa", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<String, Integer>> result = new ArrayList<>();
        wormholeForIntValue.scanRange(
            "", "aaaab", false, (k, v) -> result.add(new KeyValue<>(k, v)));
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
    }

    @Test
    void whenFunctionReturnsFalse_ShouldStopIterate() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");
      wormholeForStrValue.put("John", "nhoj");
      wormholeForStrValue.put("Jason", "nosaj");

      // Act & Assert
      KeyValue<String, String> firstItem = new KeyValue<>("James", "semaj");
      KeyValue<String, String> secondItem = new KeyValue<>("Jason", "nosaj");

      List<KeyValue<String, String>> result = new ArrayList<>();
      wormholeForStrValue.scanRange(
          "",
          null,
          false,
          (k, v) -> {
            result.add(new KeyValue<>(k, v));
            return !k.equals("Jason");
          });
      assertThat(result).containsExactly(firstItem, secondItem);
    }

    @EnabledIf("isLeafNodeLargeEnough")
    @Test
    void withManyLeafNodes_ShouldReturnThem() {
      // Arrange
      Validator<String, Integer> validator = new Validator<>(wormholeForIntValue);
      int minKeyLength = 4;
      int maxKeyLength = 16;
      // FIXME
      // int recordCount = 50000;
      int recordCount = 5000;
      TreeMap<String, Integer> expected = new TreeMap<>();
      List<String> keys = new ArrayList<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        String key = genRandomKey(minKeyLength, maxKeyLength);
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        keys.add(key);
        wormholeForIntValue.put(key, value);
      }
      validator.validate();
      Collections.sort(keys);

      // Act & Assert
      List<Map.Entry<String, Integer>> expectedKeyValues = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        int count = ThreadLocalRandom.current().nextInt(10000);
        {
          String key = genRandomKey(minKeyLength, maxKeyLength);
          expectedKeyValues.clear();
          for (Map.Entry<String, Integer> entry :
              expected.subMap(key, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").entrySet()) {
            if (expectedKeyValues.size() >= count) {
              break;
            }
            expectedKeyValues.add(entry);
          }

          List<Map.Entry<String, Integer>> actual =
              wormholeForIntValue.scanWithCount(key, count).stream()
                  .map(kv -> new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue()))
                  .collect(Collectors.toList());

          assertThat(actual).containsExactlyElementsOf(expectedKeyValues);
        }
        {
          int startIndex = ThreadLocalRandom.current().nextInt(keys.size());
          int endIndex = Math.min(startIndex + count, keys.size() - 1);
          String startKey = keys.get(startIndex);
          if (i % 2 == 0) {
            startKey += 'a';
          }
          String endKey = keys.get(endIndex);
          if (i % 3 == 0) {
            endKey += 'a';
          }
          if (startKey.compareTo(endKey) > 0) {
            endKey = startKey;
          }
          // With exclusive end keys.
          {
            expectedKeyValues.clear();
            expectedKeyValues.addAll(expected.subMap(startKey, endKey).entrySet());

            List<Map.Entry<String, Integer>> actualKeyValues = new ArrayList<>(count);
            wormholeForIntValue.scanRange(
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

            List<Map.Entry<String, Integer>> actualKeyValues = new ArrayList<>(count);
            wormholeForIntValue.scanRange(
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
    void withOneRecord_GivenSameEmptyKey_ShouldReturnTrueAndDeleteIt() {
      // Arrange
      wormholeForStrValue.put("", "foo");

      // Act & Assert
      assertThat(wormholeForStrValue.delete("")).isTrue();

      assertThat(wormholeForStrValue.scanWithCount("", 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenSameKey_ShouldReturnTrueAndDeleteIt() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");

      // Act & Assert
      assertThat(wormholeForStrValue.delete("James")).isTrue();
      assertThat(wormholeForStrValue.get("James")).isNull();
      assertThat(wormholeForStrValue.get("Jame")).isNull();
      assertThat(wormholeForStrValue.get("Jamez")).isNull();
      assertThat(wormholeForStrValue.get("Jamesa")).isNull();

      assertThat(wormholeForStrValue.scanWithCount("", 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenDifferentKey_ShouldReturnFalseAndNotDeleteIt() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");

      // Act & Assert
      assertThat(wormholeForStrValue.delete("J")).isFalse();
      assertThat(wormholeForStrValue.delete("Ja")).isFalse();
      assertThat(wormholeForStrValue.delete("Jam")).isFalse();
      assertThat(wormholeForStrValue.delete("Jame")).isFalse();
      assertThat(wormholeForStrValue.delete("Jamesa")).isFalse();
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");

      assertThat(wormholeForStrValue.scanWithCount("", 100000))
          .containsExactly(new KeyValue<>("James", "semaj"));
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldDeleteThem() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");
      wormholeForStrValue.put("John", "nhoj");
      wormholeForStrValue.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormholeForStrValue.delete("Jason")).isTrue();
      assertThat(wormholeForStrValue.get("Jason")).isNull();
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");
      assertThat(wormholeForStrValue.get("John")).isEqualTo("nhoj");
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormholeForStrValue.delete("John")).isTrue();
      assertThat(wormholeForStrValue.get("Jason")).isNull();
      assertThat(wormholeForStrValue.get("John")).isNull();
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormholeForStrValue.delete("James")).isTrue();
      assertThat(wormholeForStrValue.get("Jason")).isNull();
      assertThat(wormholeForStrValue.get("John")).isNull();
      assertThat(wormholeForStrValue.get("James")).isNull();
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_WhenDeletingFromFirstLeaf_ShouldDeleteThem() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");
      wormholeForStrValue.put("Joseph", "hpesoj");
      wormholeForStrValue.put("John", "nhoj");
      wormholeForStrValue.put("Jacob", "bocaj");
      wormholeForStrValue.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormholeForStrValue.delete("Jacob")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isNull();
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");
      assertThat(wormholeForStrValue.get("Jason")).isEqualTo("nosaj");
      assertThat(wormholeForStrValue.get("John")).isEqualTo("nhoj");
      assertThat(wormholeForStrValue.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(4);

      assertThat(wormholeForStrValue.delete("James")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isNull();
      assertThat(wormholeForStrValue.get("James")).isNull();
      assertThat(wormholeForStrValue.get("Jason")).isEqualTo("nosaj");
      assertThat(wormholeForStrValue.get("John")).isEqualTo("nhoj");
      assertThat(wormholeForStrValue.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(3);

      assertThat(wormholeForStrValue.delete("Jason")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isNull();
      assertThat(wormholeForStrValue.get("James")).isNull();
      assertThat(wormholeForStrValue.get("Jason")).isNull();
      assertThat(wormholeForStrValue.get("John")).isEqualTo("nhoj");
      assertThat(wormholeForStrValue.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormholeForStrValue.delete("John")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isNull();
      assertThat(wormholeForStrValue.get("James")).isNull();
      assertThat(wormholeForStrValue.get("Jason")).isNull();
      assertThat(wormholeForStrValue.get("John")).isNull();
      assertThat(wormholeForStrValue.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormholeForStrValue.delete("Joseph")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isNull();
      assertThat(wormholeForStrValue.get("James")).isNull();
      assertThat(wormholeForStrValue.get("Jason")).isNull();
      assertThat(wormholeForStrValue.get("John")).isNull();
      assertThat(wormholeForStrValue.get("Joseph")).isNull();
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_WhenDeletingFromLastLeaf_ShouldDeleteThem() {
      // Arrange
      wormholeForStrValue.put("James", "semaj");
      wormholeForStrValue.put("Joseph", "hpesoj");
      wormholeForStrValue.put("John", "nhoj");
      wormholeForStrValue.put("Jacob", "bocaj");
      wormholeForStrValue.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormholeForStrValue.delete("Joseph")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");
      assertThat(wormholeForStrValue.get("Jason")).isEqualTo("nosaj");
      assertThat(wormholeForStrValue.get("John")).isEqualTo("nhoj");
      assertThat(wormholeForStrValue.get("Joseph")).isNull();
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(4);

      assertThat(wormholeForStrValue.delete("John")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");
      assertThat(wormholeForStrValue.get("Jason")).isEqualTo("nosaj");
      assertThat(wormholeForStrValue.get("John")).isNull();
      assertThat(wormholeForStrValue.get("Joseph")).isNull();
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(3);

      assertThat(wormholeForStrValue.delete("Jason")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormholeForStrValue.get("James")).isEqualTo("semaj");
      assertThat(wormholeForStrValue.get("Jason")).isNull();
      assertThat(wormholeForStrValue.get("John")).isNull();
      assertThat(wormholeForStrValue.get("Joseph")).isNull();
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormholeForStrValue.delete("James")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormholeForStrValue.get("James")).isNull();
      assertThat(wormholeForStrValue.get("Jason")).isNull();
      assertThat(wormholeForStrValue.get("John")).isNull();
      assertThat(wormholeForStrValue.get("Joseph")).isNull();
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormholeForStrValue.delete("Jacob")).isTrue();
      assertThat(wormholeForStrValue.get("Jacob")).isNull();
      assertThat(wormholeForStrValue.get("James")).isNull();
      assertThat(wormholeForStrValue.get("Jason")).isNull();
      assertThat(wormholeForStrValue.get("John")).isNull();
      assertThat(wormholeForStrValue.get("Joseph")).isNull();
      assertThat(wormholeForStrValue.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_UsingSameCharForKey_WhenDeletingFromFirstLeaf_ShouldDeleteThem() {
      // Arrange
      wormholeForIntValue.put("aaaaa", 5);
      wormholeForIntValue.put("a", 1);
      wormholeForIntValue.put("aaa", 3);
      wormholeForIntValue.put("aaaa", 4);
      wormholeForIntValue.put("aa", 2);

      // Act & Assert
      assertThat(wormholeForIntValue.delete("a")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isNull();
      assertThat(wormholeForIntValue.get("aa")).isEqualTo(2);
      assertThat(wormholeForIntValue.get("aaa")).isEqualTo(3);
      assertThat(wormholeForIntValue.get("aaaa")).isEqualTo(4);
      assertThat(wormholeForIntValue.get("aaaaa")).isEqualTo(5);
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(4);

      assertThat(wormholeForIntValue.delete("aa")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isNull();
      assertThat(wormholeForIntValue.get("aa")).isNull();
      assertThat(wormholeForIntValue.get("aaa")).isEqualTo(3);
      assertThat(wormholeForIntValue.get("aaaa")).isEqualTo(4);
      assertThat(wormholeForIntValue.get("aaaaa")).isEqualTo(5);
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(3);

      assertThat(wormholeForIntValue.delete("aaa")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isNull();
      assertThat(wormholeForIntValue.get("aa")).isNull();
      assertThat(wormholeForIntValue.get("aaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaa")).isEqualTo(4);
      assertThat(wormholeForIntValue.get("aaaaa")).isEqualTo(5);
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormholeForIntValue.delete("aaaa")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isNull();
      assertThat(wormholeForIntValue.get("aa")).isNull();
      assertThat(wormholeForIntValue.get("aaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaaa")).isEqualTo(5);
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormholeForIntValue.delete("aaaaa")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isNull();
      assertThat(wormholeForIntValue.get("aa")).isNull();
      assertThat(wormholeForIntValue.get("aaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaaa")).isNull();
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_UsingSameCharForKey_WhenDeletingFromLastLeaf_ShouldDeleteThem() {
      // Arrange
      wormholeForIntValue.put("aaaaa", 5);
      wormholeForIntValue.put("a", 1);
      wormholeForIntValue.put("aaa", 3);
      wormholeForIntValue.put("aaaa", 4);
      wormholeForIntValue.put("aa", 2);

      // Act & Assert
      assertThat(wormholeForIntValue.delete("aaaaa")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isEqualTo(1);
      assertThat(wormholeForIntValue.get("aa")).isEqualTo(2);
      assertThat(wormholeForIntValue.get("aaa")).isEqualTo(3);
      assertThat(wormholeForIntValue.get("aaaa")).isEqualTo(4);
      assertThat(wormholeForIntValue.get("aaaaa")).isNull();
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(4);

      assertThat(wormholeForIntValue.delete("aaaa")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isEqualTo(1);
      assertThat(wormholeForIntValue.get("aa")).isEqualTo(2);
      assertThat(wormholeForIntValue.get("aaa")).isEqualTo(3);
      assertThat(wormholeForIntValue.get("aaaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaaa")).isNull();
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(3);

      assertThat(wormholeForIntValue.delete("aaa")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isEqualTo(1);
      assertThat(wormholeForIntValue.get("aa")).isEqualTo(2);
      assertThat(wormholeForIntValue.get("aaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaaa")).isNull();
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormholeForIntValue.delete("aa")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isEqualTo(1);
      assertThat(wormholeForIntValue.get("aa")).isNull();
      assertThat(wormholeForIntValue.get("aaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaaa")).isNull();
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormholeForIntValue.delete("a")).isTrue();
      assertThat(wormholeForIntValue.get("a")).isNull();
      assertThat(wormholeForIntValue.get("aa")).isNull();
      assertThat(wormholeForIntValue.get("aaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaa")).isNull();
      assertThat(wormholeForIntValue.get("aaaaa")).isNull();
      assertThat(wormholeForIntValue.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @EnabledIf("isLeafNodeLargeEnough")
    @Test
    void withManyLeafNodes_ShouldReturnIt() {
      // Arrange
      Validator<String, Integer> validator = new Validator<>(wormholeForIntValue);
      int minKeyLength = 4;
      int maxKeyLength = 16;
      // FIXME
      // int recordCount = 30000;
      int recordCount = 3000;
      Map<String, Integer> expected = new HashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        String key = genRandomKey(minKeyLength, maxKeyLength);
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormholeForIntValue.put(key, value);
      }
      List<String> expectedKeys = new ArrayList<>(expected.keySet());

      // Act & Assert

      // 100% -> 50%
      for (int i = 0; i < expected.size() - recordCount * 0.5; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        String key = expectedKeys.get(keyIndex);
        assertThat(wormholeForIntValue.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        assertThat(wormholeForIntValue.get(entry.getKey())).isEqualTo(entry.getValue());
      }

      // 50% -> 5%
      for (int i = 0; i < expected.size() - recordCount * 0.05; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        String key = expectedKeys.get(keyIndex);
        assertThat(wormholeForIntValue.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      Map<String, Integer> scanned =
          wormholeForIntValue.scanWithCount("", 100000).stream()
              .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        String key = entry.getKey();
        Integer actualValue = wormholeForIntValue.get(key);
        assertThat(actualValue).isEqualTo(entry.getValue());
        assertThat(scanned.get(key)).isEqualTo(entry.getValue());
      }

      // 100 -> 0
      int remainingCount = expected.size();
      for (int i = 0; i < remainingCount; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        String key = expectedKeys.get(keyIndex);
        assertThat(wormholeForIntValue.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      assertThat(wormholeForIntValue.scanWithCount("", 100000)).isEmpty();
    }
  }
}
