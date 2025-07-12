package org.komamitsu.wormhole;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class WormholeTest {
  @Nested
  class Get {
    @Test
    void afterPutting1Record_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(3);
      wormhole.put("James", "semaj");

      // Act & Assert
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("Jame")).isNull();
      assertThat(wormhole.get("Jamez")).isNull();
      assertThat(wormhole.get("Jamesa")).isNull();
    }

    @Test
    void afterPuttingMaxRecordsPerPartition_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(3);
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
    void afterPuttingMoreThanRecordsPerPartition_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(3);
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
    void afterPuttingMoreThanRecordsUsingSameCharPerPartition_ShouldReturnIt() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(3);
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

    @Test
    void afterPuttingManyRecords_ShouldReturnIt() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(3);
      int maxKeyLength = 8;
      int recordCount = 50000;
      Map<String, Integer> expected = new LinkedHashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        // TODO: NPE occurs when the key length is 0.
        int keyLength = ThreadLocalRandom.current().nextInt(1, maxKeyLength + 1);
        StringBuilder sb = new StringBuilder(keyLength);
        for (int j = 0; j < keyLength; j++) {
          char c = (char) ThreadLocalRandom.current().nextInt('a', 'z' + 1);
          sb.append(c);
        }
        String key = sb.toString();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormhole.put(key, value);
      }

      // Act & Assert
      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        assertThat(wormhole.get(entry.getKey())).isEqualTo(entry.getValue());
      }
    }
  }

  @Nested
  class Scan {
    @Test
    void afterPutting1Record_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(3);
      wormhole.put("James", "semaj");

      // Act & Assert
      KeyValue<String> firstItem = new KeyValue<>("James", "semaj");
      assertThat(wormhole.scan("J", 0)).isEmpty();
      assertThat(wormhole.scan("J", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("J", 2)).containsExactly(firstItem);
      assertThat(wormhole.scan("Ja", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Ja", 2)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jam", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jam", 2)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jame", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jame", 2)).containsExactly(firstItem);
      assertThat(wormhole.scan("James", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("James", 2)).containsExactly(firstItem);
      assertThat(wormhole.scan("I", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("I", 2)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jal", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jal", 2)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jamd", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jamd", 2)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jamer", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jamer", 2)).containsExactly(firstItem);
      assertThat(wormhole.scan("K", 2)).isEmpty();
      assertThat(wormhole.scan("Jb", 2)).isEmpty();
      assertThat(wormhole.scan("Jan", 2)).isEmpty();
      assertThat(wormhole.scan("Jamf", 2)).isEmpty();
      assertThat(wormhole.scan("Jamet", 2)).isEmpty();
    }

    @Test
    void afterPuttingMaxRecordsPerPartition_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(3);
      wormhole.put("James", "semaj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jason", "nosaj");

      // Act & Assert
      KeyValue<String> firstItem = new KeyValue<>("James", "semaj");
      KeyValue<String> secondItem = new KeyValue<>("Jason", "nosaj");
      KeyValue<String> thirdItem = new KeyValue<>("John", "nhoj");
      assertThat(wormhole.scan("I", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("I", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("I", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("I", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("J", 0)).isEmpty();
      assertThat(wormhole.scan("J", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("J", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("J", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("J", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Ja", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Ja", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("Ja", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Ja", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jal", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jal", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("Jal", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jal", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jam", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jam", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("Jam", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jam", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jan", 1)).containsExactly(secondItem);
      assertThat(wormhole.scan("Jan", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jan", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jamd", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jamd", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("Jamd", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jamd", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jame", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jame", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("Jame", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jame", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jamf", 1)).containsExactly(secondItem);
      assertThat(wormhole.scan("Jamf", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jamf", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jamer", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Jamer", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("Jamer", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jamer", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("James", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("James", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("James", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("James", 4)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Jamet", 1)).containsExactly(secondItem);
      assertThat(wormhole.scan("Jamet", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jamet", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jar", 1)).containsExactly(secondItem);
      assertThat(wormhole.scan("Jar", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jar", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jas", 1)).containsExactly(secondItem);
      assertThat(wormhole.scan("Jas", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jas", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jat", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jat", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jasn", 1)).containsExactly(secondItem);
      assertThat(wormhole.scan("Jasn", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jasn", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jaso", 1)).containsExactly(secondItem);
      assertThat(wormhole.scan("Jaso", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jaso", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jasp", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jasp", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jasol", 1)).containsExactly(secondItem);
      assertThat(wormhole.scan("Jasol", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jasol", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jason", 1)).containsExactly(secondItem);
      assertThat(wormhole.scan("Jason", 2)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jason", 3)).containsExactly(secondItem, thirdItem);
      assertThat(wormhole.scan("Jasoo", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jasoo", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jn", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jn", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jo", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jo", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jp", 1)).isEmpty();
      assertThat(wormhole.scan("Jog", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jog", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Joh", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Joh", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Joi", 1)).isEmpty();
      assertThat(wormhole.scan("Johm", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Johm", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scan("John", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("John", 2)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Joho", 1)).isEmpty();
      assertThat(wormhole.scan("K", 1)).isEmpty();
    }

    @Test
    void afterPuttingMoreThanRecordsPerPartition_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(3);
      wormhole.put("James", "semaj");
      wormhole.put("Joseph", "hpesoj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jacob", "bocaj");
      wormhole.put("Jason", "nosaj");

      // Act & Assert
      KeyValue<String> firstItem = new KeyValue<>("Jacob", "bocaj");
      KeyValue<String> secondItem = new KeyValue<>("James", "semaj");
      KeyValue<String> thirdItem = new KeyValue<>("Jason", "nosaj");
      KeyValue<String> fourthItem = new KeyValue<>("John", "nhoj");
      KeyValue<String> fifthItem = new KeyValue<>("Joseph", "hpesoj");
      assertThat(wormhole.scan("I", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("I", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("I", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("I", 4)).containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scan("I", 5)).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scan("J", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("J", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("J", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("J", 4)).containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scan("J", 5)).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scan("Ja", 1)).containsExactly(firstItem);
      assertThat(wormhole.scan("Ja", 2)).containsExactly(firstItem, secondItem);
      assertThat(wormhole.scan("Ja", 3)).containsExactly(firstItem, secondItem, thirdItem);
      assertThat(wormhole.scan("Ja", 4)).containsExactly(firstItem, secondItem, thirdItem, fourthItem);
      assertThat(wormhole.scan("Ja", 5)).containsExactly(firstItem, secondItem, thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scan("Jasom", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jasom", 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scan("Jasom", 3)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scan("Jason", 1)).containsExactly(thirdItem);
      assertThat(wormhole.scan("Jason", 2)).containsExactly(thirdItem, fourthItem);
      assertThat(wormhole.scan("Jason", 3)).containsExactly(thirdItem, fourthItem, fifthItem);
      assertThat(wormhole.scan("Jasoo", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scan("Jasoo", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scan("Jb", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scan("Jb", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scan("Jm", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scan("Jm", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scan("Jo", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scan("Jo", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scan("Jog", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scan("Jog", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scan("Joh", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scan("Joh", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scan("Johm", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scan("Johm", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scan("John", 1)).containsExactly(fourthItem);
      assertThat(wormhole.scan("John", 2)).containsExactly(fourthItem, fifthItem);
      assertThat(wormhole.scan("Joho", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scan("Jor", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scan("Jos", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scan("Josd", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scan("Jose", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scan("Joseo", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scan("Josep", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scan("Josepg", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scan("Joseph", 1)).containsExactly(fifthItem);
      assertThat(wormhole.scan("Josepha", 1)).isEmpty();
      assertThat(wormhole.scan("Josepi", 1)).isEmpty();
      assertThat(wormhole.scan("Joseq", 1)).isEmpty();
      assertThat(wormhole.scan("Josf", 1)).isEmpty();
      assertThat(wormhole.scan("Jot", 1)).isEmpty();
      assertThat(wormhole.scan("Jp", 1)).isEmpty();
      assertThat(wormhole.scan("K", 1)).isEmpty();
    }

    @Test
    void afterPuttingMoreThanRecordsUsingSameCharPerPartition_ShouldReturnIt() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(3);
      wormhole.put("aaaaa", 5);
      wormhole.put("a", 1);
      wormhole.put("aaa", 3);
      wormhole.put("aaaa", 4);
      wormhole.put("aa", 2);

      // Act & Assert

      // FIXME
    }

    @Test
    void afterPuttingManyRecords_ShouldReturnIt() {
      // Arrange
      Wormhole<Integer> wormhole = new Wormhole<>(3);
      int maxKeyLength = 8;
      int recordCount = 50000;
      Map<String, Integer> expected = new LinkedHashMap<>(recordCount);
      for (int i = 0; i < recordCount; i++) {
        // TODO: NPE occurs when the key length is 0.
        int keyLength = ThreadLocalRandom.current().nextInt(1, maxKeyLength + 1);
        StringBuilder sb = new StringBuilder(keyLength);
        for (int j = 0; j < keyLength; j++) {
          char c = (char) ThreadLocalRandom.current().nextInt('a', 'z' + 1);
          sb.append(c);
        }
        String key = sb.toString();
        int value = ThreadLocalRandom.current().nextInt();
        expected.put(key, value);
        wormhole.put(key, value);
      }

      // Act & Assert

      // FIXME
    }
  }
}