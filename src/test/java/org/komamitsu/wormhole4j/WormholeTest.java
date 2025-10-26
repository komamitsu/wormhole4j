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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;
import org.komamitsu.wormhole4j.WormholeBase.Validator;

@ParameterizedClass
@ValueSource(ints = {3, 8, 128})
class WormholeTest {
  @Parameter int leafNodeSize;

  class Common {
    boolean isLeafNodeLargeEnough() {
      return leafNodeSize >= 8;
    }
  }

  @Nested
  class Get extends Common {
    @Test
    void withOneLeafNodeWithOneEmptyRecord_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("", "foo");

      // Act & Assert
      assertThat(wormhole.get("")).isEqualTo("foo");
    }

    @Test
    void withOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");

      // Act & Assert
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("Jame")).isNull();
      assertThat(wormhole.get("Jamez")).isNull();
      assertThat(wormhole.get("Jamesa")).isNull();
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("Jame")).isNull();
      assertThat(wormhole.get("Jason")).isEqualTo("nosaj");
      assertThat(wormhole.get("Jasona")).isNull();
      assertThat(wormhole.get("John")).isEqualTo("nhoj");
      assertThat(wormhole.get("Johh")).isNull();
    }

    @Test
    void withTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");
      wormhole.put("Joseph", "hpesoj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jacob", "bocaj");
      wormhole.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormhole.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormhole.get("Jaco")).isNull();
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("Jame")).isNull();
      assertThat(wormhole.get("Jason")).isEqualTo("nosaj");
      assertThat(wormhole.get("Jasona")).isNull();
      assertThat(wormhole.get("John")).isEqualTo("nhoj");
      assertThat(wormhole.get("Johh")).isNull();
      assertThat(wormhole.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormhole.get("Josepha")).isNull();
    }

    @Test
    void withTwoLeafNodes_UsingSameCharForKey_ShouldReturnIt() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("aaaaa", 5);
      wormhole.put("a", 1);
      wormhole.put("aaa", 3);
      wormhole.put("aaaa", 4);
      wormhole.put("aa", 2);

      // Act & Assert
      assertThat(wormhole.get("a")).isEqualTo(1);
      assertThat(wormhole.get("aa")).isEqualTo(2);
      assertThat(wormhole.get("aaa")).isEqualTo(3);
      assertThat(wormhole.get("aaaa")).isEqualTo(4);
      assertThat(wormhole.get("aaaaa")).isEqualTo(5);
    }

    @EnabledIf("isLeafNodeLargeEnough")
    @Test
    void withManyLeafNodes_ShouldReturnThem() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(leafNodeSize);
      Validator<String, ByteArrayEncodedKey, Integer> validator = new Validator<>(wormhole);
      int minKeyLength = 4;
      int maxKeyLength = 16;
      int recordCount = 50000;
      Map<String, Integer> expected = new LinkedHashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        String key = genRandomKey(minKeyLength, maxKeyLength);
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormhole.put(key, value);
      }
      validator.validate();

      // Act & Assert
      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        assertThat(wormhole.get(entry.getKey())).isEqualTo(entry.getValue());
      }
    }
  }

  @Nested
  class Scan extends Common {
    @Test
    void withOneLeafNodeWithOneEmptyRecord_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("", "foo");

      // Act & Assert
      KeyValue<String, ByteArrayEncodedKey, String> firstItem =
          new KeyValue<>(createForTest(wormhole, ""), "foo");
      assertThat(wormhole.scanWithCount("", 2)).containsExactly(firstItem);

      // With exclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("", "", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("", null, result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey(null, "", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey(null, null, result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("a", "z", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("a", null, result::add);
        assertThat(result).isEmpty();
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("", "", result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("", null, result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey(null, "", result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey(null, null, result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("a", "z", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("a", null, result::add);
        assertThat(result).isEmpty();
      }
    }

    @Test
    void withOneLeafNodeWithOneRecord_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");

      // Act & Assert
      KeyValue<String, ByteArrayEncodedKey, String> firstItem =
          new KeyValue<>(createForTest(wormhole, "James"), "semaj");
      assertThat(wormhole.scanWithCount("J", 0)).isEmpty();
      assertThat(wormhole.scanWithCount("J", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("J", 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Ja", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Ja", 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jam", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jam", 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jame", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jame", 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("James", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("James", 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("I", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("I", 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jal", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jal", 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jamd", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jamd", 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jamer", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jamer", 2)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("K", 2)).isEmpty();
      assertThat(wormhole.scanWithCount("Jb", 2)).isEmpty();
      assertThat(wormhole.scanWithCount("Jan", 2)).isEmpty();
      assertThat(wormhole.scanWithCount("Jamf", 2)).isEmpty();
      assertThat(wormhole.scanWithCount("Jamet", 2)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("James", "James", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey(null, "James", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("James", null, result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("Jamesa", "Jamesa", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey(null, "Jamer", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("Jamesa", null, result::add);
        assertThat(result).isEmpty();
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("James", "James", result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey(null, "James", result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("James", null, result::add);
        assertThat(result).containsExactly(firstItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("Jamesa", "Jamesa", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey(null, "Jamer", result::add);
        assertThat(result).isEmpty();
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("Jamesa", null, result::add);
        assertThat(result).isEmpty();
      }
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jason", "nosaj");

      // Act & Assert
      KeyValue<String, ByteArrayEncodedKey, String> firstItem =
          new KeyValue<>(createForTest(wormhole, "James"), "semaj");
      KeyValue<String, ByteArrayEncodedKey, String> secondItem =
          new KeyValue<>(createForTest(wormhole, "Jason"), "nosaj");
      KeyValue<String, ByteArrayEncodedKey, String> thirdItem =
          new KeyValue<>(createForTest(wormhole, "John"), "nhoj");
      assertThat(wormhole.scanWithCount("I", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("I", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("I", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("I", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("J", 0)).isEmpty();
      assertThat(wormhole.scanWithCount("J", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("J", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("J", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("J", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Ja", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Ja", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("Ja", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Ja", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jal", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jal", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("Jal", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jal", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jam", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jam", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("Jam", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jam", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jan", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("Jan", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jan", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jamd", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jamd", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("Jamd", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jamd", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jame", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jame", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("Jame", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jame", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jamf", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("Jamf", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jamf", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jamer", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Jamer", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("Jamer", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jamer", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("James", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("James", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("James", 3))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("James", 4))
          .containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jamet", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("Jamet", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jamet", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jar", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("Jar", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jar", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jas", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("Jas", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jas", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jat", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jat", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jasn", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("Jasn", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jasn", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jaso", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("Jaso", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jaso", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jasp", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jasp", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jasol", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("Jasol", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jasol", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jason", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("Jason", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jason", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Jasoo", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jasoo", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jn", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jn", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jo", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jo", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jp", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("Jog", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jog", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Joh", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Joh", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Joi", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("Johm", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Johm", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("John", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("John", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Joho", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("K", 1)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("James", "John", result::add);
        assertThat(result).containsExactly(firstItem, secondItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("Jamer", "Johna", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("Jamesa", "Johm", result::add);
        assertThat(result).containsExactly(secondItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("James", "John", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("Jamer", "Johna", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("Jamesa", "Johm", result::add);
        assertThat(result).containsExactly(secondItem);
      }
    }

    @Test
    void withTwoLeafNodes_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");
      wormhole.put("Joseph", "hpesoj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jacob", "bocaj");
      wormhole.put("Jason", "nosaj");

      // Act & Assert
      KeyValue<String, ByteArrayEncodedKey, String> firstItem =
          new KeyValue<>(createForTest(wormhole, "Jacob"), "bocaj");
      KeyValue<String, ByteArrayEncodedKey, String> secondItem =
          new KeyValue<>(createForTest(wormhole, "James"), "semaj");
      KeyValue<String, ByteArrayEncodedKey, String> thirdItem =
          new KeyValue<>(createForTest(wormhole, "Jason"), "nosaj");
      KeyValue<String, ByteArrayEncodedKey, String> fourthItem =
          new KeyValue<>(createForTest(wormhole, "John"), "nhoj");
      KeyValue<String, ByteArrayEncodedKey, String> fifthItem =
          new KeyValue<>(createForTest(wormhole, "Joseph"), "hpesoj");
      assertThat(wormhole.scanWithCount("I", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("I", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("I", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("I", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount("I", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("J", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("J", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("J", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("J", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount("J", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Ja", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("Ja", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("Ja", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("Ja", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount("Ja", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Jasom", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jasom", 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount("Jasom", 3))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Jason", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("Jason", 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount("Jason", 3))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Jasoo", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount("Jasoo", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Jb", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount("Jb", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Jm", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount("Jm", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Jo", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount("Jo", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Jog", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount("Jog", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Joh", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount("Joh", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Johm", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount("Johm", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("John", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount("John", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("Joho", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("Jor", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("Jos", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("Josd", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("Jose", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("Joseo", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("Josep", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("Josepg", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("Joseph", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("Josepha", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("Josepi", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("Joseq", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("Josf", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("Jot", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("Jp", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("K", 1)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("Jacob", "Joseph", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("Jacoa", "Josepha", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("Jacoba", "Josepg", result::add);
        assertThat(result).containsExactly(secondItem, thirdItem, fourthItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("Jacob", "Joseph", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("Jacoa", "Josepha", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, String>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("Jacoba", "Josepg", result::add);
        assertThat(result).containsExactly(secondItem, thirdItem, fourthItem);
      }
    }

    @Test
    void withTwoLeafNodes_UsingSameCharForKey_ShouldReturnIt() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("aaaaa", 5);
      wormhole.put("a", 1);
      wormhole.put("aaa", 3);
      wormhole.put("aaaa", 4);
      wormhole.put("aa", 2);

      // Act & Assert
      KeyValue<String, ByteArrayEncodedKey, Integer> firstItem =
          new KeyValue<>(createForTest(wormhole, "a"), 1);
      KeyValue<String, ByteArrayEncodedKey, Integer> secondItem =
          new KeyValue<>(createForTest(wormhole, "aa"), 2);
      KeyValue<String, ByteArrayEncodedKey, Integer> thirdItem =
          new KeyValue<>(createForTest(wormhole, "aaa"), 3);
      KeyValue<String, ByteArrayEncodedKey, Integer> fourthItem =
          new KeyValue<>(createForTest(wormhole, "aaaa"), 4);
      KeyValue<String, ByteArrayEncodedKey, Integer> fifthItem =
          new KeyValue<>(createForTest(wormhole, "aaaaa"), 5);

      assertThat(wormhole.scanWithCount("", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount("", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("a", 1)).containsExactly(firstItem);
      assertThat(wormhole.scanWithCount("a", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scanWithCount("a", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("a", 4))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount("a", 5))
          .containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("aa", 1)).containsExactly(secondItem);
      assertThat(wormhole.scanWithCount("aa", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scanWithCount("aa", 3))
          .containsExactly(secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount("aa", 4))
          .containsExactly(secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("ab", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("aaa", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scanWithCount("aaa", 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scanWithCount("aaa", 3))
          .containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("aab", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("aaaa", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scanWithCount("aaaa", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scanWithCount("aaab", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("aaaaa", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scanWithCount("aaaab", 1)).isEmpty();
      assertThat(wormhole.scanWithCount("b", 1)).isEmpty();
      // With exclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, Integer>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("a", "aaaaa", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, Integer>> result = new ArrayList<>();
        wormhole.scanWithExclusiveEndKey("", "aaaab", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      // With inclusive end keys.
      {
        List<KeyValue<String, ByteArrayEncodedKey, Integer>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("a", "aaaaa", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
      {
        List<KeyValue<String, ByteArrayEncodedKey, Integer>> result = new ArrayList<>();
        wormhole.scanWithInclusiveEndKey("", "aaaab", result::add);
        assertThat(result).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      }
    }

    @EnabledIf("isLeafNodeLargeEnough")
    @Test
    void withManyLeafNodes_ShouldReturnThem() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(leafNodeSize);
      Validator<String, ByteArrayEncodedKey, Integer> validator = new Validator<>(wormhole);
      int minKeyLength = 4;
      int maxKeyLength = 16;
      int recordCount = 50000;
      TreeMap<String, Integer> expected = new TreeMap<>();
      List<String> keys = new ArrayList<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        String key = genRandomKey(minKeyLength, maxKeyLength);
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        keys.add(key);
        wormhole.put(key, value);
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
              wormhole.scanWithCount(key, count).stream()
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
            wormhole.scanWithExclusiveEndKey(
                startKey,
                endKey,
                kv -> {
                  actualKeyValues.add(new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue()));
                  return true;
                });

            assertThat(actualKeyValues).containsExactlyElementsOf(expectedKeyValues);
          }
          // With inclusive end keys.
          {
            expectedKeyValues.clear();
            expectedKeyValues.addAll(expected.subMap(startKey, true, endKey, true).entrySet());

            List<Map.Entry<String, Integer>> actualKeyValues = new ArrayList<>(count);
            wormhole.scanWithInclusiveEndKey(
                startKey,
                endKey,
                kv -> {
                  actualKeyValues.add(new AbstractMap.SimpleEntry<>(kv.getKey(), kv.getValue()));
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
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("", "foo");

      // Act & Assert
      assertThat(wormhole.delete("")).isTrue();

      assertThat(wormhole.scanWithCount("", 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenSameKey_ShouldReturnTrueAndDeleteIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");

      // Act & Assert
      assertThat(wormhole.delete("James")).isTrue();
      assertThat(wormhole.get("James")).isNull();
      assertThat(wormhole.get("Jame")).isNull();
      assertThat(wormhole.get("Jamez")).isNull();
      assertThat(wormhole.get("Jamesa")).isNull();

      assertThat(wormhole.scanWithCount("", 100000)).isEmpty();
    }

    @Test
    void withOneRecord_GivenDifferentKey_ShouldReturnFalseAndNotDeleteIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");

      // Act & Assert
      assertThat(wormhole.delete("J")).isFalse();
      assertThat(wormhole.delete("Ja")).isFalse();
      assertThat(wormhole.delete("Jam")).isFalse();
      assertThat(wormhole.delete("Jame")).isFalse();
      assertThat(wormhole.delete("Jamesa")).isFalse();
      assertThat(wormhole.get("James")).isEqualTo("semaj");

      assertThat(wormhole.scanWithCount("", 100000))
          .containsExactly(new KeyValue<>(createForTest(wormhole, "James"), "semaj"));
    }

    @Test
    void withOneLeafNodeWithMaxRecords_ShouldDeleteThem() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormhole.delete("Jason")).isTrue();
      assertThat(wormhole.get("Jason")).isNull();
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("John")).isEqualTo("nhoj");
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormhole.delete("John")).isTrue();
      assertThat(wormhole.get("Jason")).isNull();
      assertThat(wormhole.get("John")).isNull();
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormhole.delete("James")).isTrue();
      assertThat(wormhole.get("Jason")).isNull();
      assertThat(wormhole.get("John")).isNull();
      assertThat(wormhole.get("James")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_WhenDeletingFromFirstLeaf_ShouldDeleteThem() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");
      wormhole.put("Joseph", "hpesoj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jacob", "bocaj");
      wormhole.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormhole.delete("Jacob")).isTrue();
      assertThat(wormhole.get("Jacob")).isNull();
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("Jason")).isEqualTo("nosaj");
      assertThat(wormhole.get("John")).isEqualTo("nhoj");
      assertThat(wormhole.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(4);

      assertThat(wormhole.delete("James")).isTrue();
      assertThat(wormhole.get("Jacob")).isNull();
      assertThat(wormhole.get("James")).isNull();
      assertThat(wormhole.get("Jason")).isEqualTo("nosaj");
      assertThat(wormhole.get("John")).isEqualTo("nhoj");
      assertThat(wormhole.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(3);

      assertThat(wormhole.delete("Jason")).isTrue();
      assertThat(wormhole.get("Jacob")).isNull();
      assertThat(wormhole.get("James")).isNull();
      assertThat(wormhole.get("Jason")).isNull();
      assertThat(wormhole.get("John")).isEqualTo("nhoj");
      assertThat(wormhole.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormhole.delete("John")).isTrue();
      assertThat(wormhole.get("Jacob")).isNull();
      assertThat(wormhole.get("James")).isNull();
      assertThat(wormhole.get("Jason")).isNull();
      assertThat(wormhole.get("John")).isNull();
      assertThat(wormhole.get("Joseph")).isEqualTo("hpesoj");
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormhole.delete("Joseph")).isTrue();
      assertThat(wormhole.get("Jacob")).isNull();
      assertThat(wormhole.get("James")).isNull();
      assertThat(wormhole.get("Jason")).isNull();
      assertThat(wormhole.get("John")).isNull();
      assertThat(wormhole.get("Joseph")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_WhenDeletingFromLastLeaf_ShouldDeleteThem() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("James", "semaj");
      wormhole.put("Joseph", "hpesoj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jacob", "bocaj");
      wormhole.put("Jason", "nosaj");

      // Act & Assert
      assertThat(wormhole.delete("Joseph")).isTrue();
      assertThat(wormhole.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("Jason")).isEqualTo("nosaj");
      assertThat(wormhole.get("John")).isEqualTo("nhoj");
      assertThat(wormhole.get("Joseph")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(4);

      assertThat(wormhole.delete("John")).isTrue();
      assertThat(wormhole.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("Jason")).isEqualTo("nosaj");
      assertThat(wormhole.get("John")).isNull();
      assertThat(wormhole.get("Joseph")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(3);

      assertThat(wormhole.delete("Jason")).isTrue();
      assertThat(wormhole.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("Jason")).isNull();
      assertThat(wormhole.get("John")).isNull();
      assertThat(wormhole.get("Joseph")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormhole.delete("James")).isTrue();
      assertThat(wormhole.get("Jacob")).isEqualTo("bocaj");
      assertThat(wormhole.get("James")).isNull();
      assertThat(wormhole.get("Jason")).isNull();
      assertThat(wormhole.get("John")).isNull();
      assertThat(wormhole.get("Joseph")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormhole.delete("Jacob")).isTrue();
      assertThat(wormhole.get("Jacob")).isNull();
      assertThat(wormhole.get("James")).isNull();
      assertThat(wormhole.get("Jason")).isNull();
      assertThat(wormhole.get("John")).isNull();
      assertThat(wormhole.get("Joseph")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_UsingSameCharForKey_WhenDeletingFromFirstLeaf_ShouldDeleteThem() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("aaaaa", 5);
      wormhole.put("a", 1);
      wormhole.put("aaa", 3);
      wormhole.put("aaaa", 4);
      wormhole.put("aa", 2);

      // Act & Assert
      assertThat(wormhole.delete("a")).isTrue();
      assertThat(wormhole.get("a")).isNull();
      assertThat(wormhole.get("aa")).isEqualTo(2);
      assertThat(wormhole.get("aaa")).isEqualTo(3);
      assertThat(wormhole.get("aaaa")).isEqualTo(4);
      assertThat(wormhole.get("aaaaa")).isEqualTo(5);
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(4);

      assertThat(wormhole.delete("aa")).isTrue();
      assertThat(wormhole.get("a")).isNull();
      assertThat(wormhole.get("aa")).isNull();
      assertThat(wormhole.get("aaa")).isEqualTo(3);
      assertThat(wormhole.get("aaaa")).isEqualTo(4);
      assertThat(wormhole.get("aaaaa")).isEqualTo(5);
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(3);

      assertThat(wormhole.delete("aaa")).isTrue();
      assertThat(wormhole.get("a")).isNull();
      assertThat(wormhole.get("aa")).isNull();
      assertThat(wormhole.get("aaa")).isNull();
      assertThat(wormhole.get("aaaa")).isEqualTo(4);
      assertThat(wormhole.get("aaaaa")).isEqualTo(5);
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormhole.delete("aaaa")).isTrue();
      assertThat(wormhole.get("a")).isNull();
      assertThat(wormhole.get("aa")).isNull();
      assertThat(wormhole.get("aaa")).isNull();
      assertThat(wormhole.get("aaaa")).isNull();
      assertThat(wormhole.get("aaaaa")).isEqualTo(5);
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormhole.delete("aaaaa")).isTrue();
      assertThat(wormhole.get("a")).isNull();
      assertThat(wormhole.get("aa")).isNull();
      assertThat(wormhole.get("aaa")).isNull();
      assertThat(wormhole.get("aaaa")).isNull();
      assertThat(wormhole.get("aaaaa")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @Test
    void withTwoLeafNodes_UsingSameCharForKey_WhenDeletingFromLastLeaf_ShouldDeleteThem() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(leafNodeSize, true);
      wormhole.put("aaaaa", 5);
      wormhole.put("a", 1);
      wormhole.put("aaa", 3);
      wormhole.put("aaaa", 4);
      wormhole.put("aa", 2);

      // Act & Assert
      assertThat(wormhole.delete("aaaaa")).isTrue();
      assertThat(wormhole.get("a")).isEqualTo(1);
      assertThat(wormhole.get("aa")).isEqualTo(2);
      assertThat(wormhole.get("aaa")).isEqualTo(3);
      assertThat(wormhole.get("aaaa")).isEqualTo(4);
      assertThat(wormhole.get("aaaaa")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(4);

      assertThat(wormhole.delete("aaaa")).isTrue();
      assertThat(wormhole.get("a")).isEqualTo(1);
      assertThat(wormhole.get("aa")).isEqualTo(2);
      assertThat(wormhole.get("aaa")).isEqualTo(3);
      assertThat(wormhole.get("aaaa")).isNull();
      assertThat(wormhole.get("aaaaa")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(3);

      assertThat(wormhole.delete("aaa")).isTrue();
      assertThat(wormhole.get("a")).isEqualTo(1);
      assertThat(wormhole.get("aa")).isEqualTo(2);
      assertThat(wormhole.get("aaa")).isNull();
      assertThat(wormhole.get("aaaa")).isNull();
      assertThat(wormhole.get("aaaaa")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(2);

      assertThat(wormhole.delete("aa")).isTrue();
      assertThat(wormhole.get("a")).isEqualTo(1);
      assertThat(wormhole.get("aa")).isNull();
      assertThat(wormhole.get("aaa")).isNull();
      assertThat(wormhole.get("aaaa")).isNull();
      assertThat(wormhole.get("aaaaa")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(1);

      assertThat(wormhole.delete("a")).isTrue();
      assertThat(wormhole.get("a")).isNull();
      assertThat(wormhole.get("aa")).isNull();
      assertThat(wormhole.get("aaa")).isNull();
      assertThat(wormhole.get("aaaa")).isNull();
      assertThat(wormhole.get("aaaaa")).isNull();
      assertThat(wormhole.scanWithCount("", 100000)).size().isEqualTo(0);
    }

    @EnabledIf("isLeafNodeLargeEnough")
    @Test
    void withManyLeafNodes_ShouldReturnIt() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(leafNodeSize);
      Validator<String, ByteArrayEncodedKey, Integer> validator = new Validator<>(wormhole);
      int minKeyLength = 4;
      int maxKeyLength = 16;
      int recordCount = 30000;
      Map<String, Integer> expected = new HashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        String key = genRandomKey(minKeyLength, maxKeyLength);
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormhole.put(key, value);
      }
      List<String> expectedKeys = new ArrayList<>(expected.keySet());

      // Act & Assert

      // 100% -> 50%
      for (int i = 0; i < expected.size() - recordCount * 0.5; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        String key = expectedKeys.get(keyIndex);
        assertThat(wormhole.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        assertThat(wormhole.get(entry.getKey())).isEqualTo(entry.getValue());
      }

      // 50% -> 5%
      for (int i = 0; i < expected.size() - recordCount * 0.05; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        String key = expectedKeys.get(keyIndex);
        assertThat(wormhole.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      Map<String, Integer> scanned =
          wormhole.scanWithCount("", 100000).stream()
              .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        String key = entry.getKey();
        Integer actualValue = wormhole.get(key);
        assertThat(actualValue).isEqualTo(entry.getValue());
        assertThat(scanned.get(key)).isEqualTo(entry.getValue());
      }

      // 100 -> 0
      int remainingCount = expected.size();
      for (int i = 0; i < remainingCount; i++) {
        int keyIndex = ThreadLocalRandom.current().nextInt(expectedKeys.size());
        String key = expectedKeys.get(keyIndex);
        assertThat(wormhole.delete(key)).isTrue();
        expected.remove(key);
        expectedKeys.remove(keyIndex);
      }
      validator.validate();

      assertThat(wormhole.scanWithCount("", 100000)).isEmpty();
    }
  }

  private <V> Key<String, ByteArrayEncodedKey> createForTest(Wormhole<V> wormhole, String key) {
    return new Key<>(key, wormhole.encodeKey(key));
  }
}
