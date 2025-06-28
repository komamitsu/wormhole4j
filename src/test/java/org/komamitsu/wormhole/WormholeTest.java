package org.komamitsu.wormhole;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.HashMap;
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

      // Act
      wormhole.put("James", "semaj");

      // Assert
      assertThat(wormhole.get("James")).isEqualTo("semaj");
      assertThat(wormhole.get("Jame")).isNull();
      assertThat(wormhole.get("Jamez")).isNull();
      assertThat(wormhole.get("Jamesa")).isNull();
    }

    @Test
    void afterPuttingMaxRecordsPerPartition_ShouldReturnIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(3);

      // Act
      wormhole.put("James", "semaj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jason", "nosaj");

      // Assert
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

      // Act
      wormhole.put("James", "semaj");
      wormhole.put("Joseph", "hpesoj");
      wormhole.put("John", "nhoj");
      wormhole.put("Jacob", "bocaj");
      wormhole.put("Jason", "nosaj");

      // Assert
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

      // Act
      wormhole.put("aaaaa", 5);
      wormhole.put("a", 1);
      wormhole.put("aaa", 3);
      wormhole.put("aaaa", 4);
      wormhole.put("aa", 2);

      // Assert
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
      WormholeValidator<Integer> validator = new WormholeValidator<>(wormhole);
      int maxKeyLength = 8;
      int recordCount = 7;
      Map<String, Integer> expected = new LinkedHashMap<>(recordCount);

      // Act
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
        validator.validate();
      }

      // Assert
      for (Map.Entry<String, Integer> entry : expected.entrySet()) {
        assertThat(entry.getValue()).isEqualTo(wormhole.get(entry.getKey()));
      }
    }
  }

  @Test
  void aaaaa() {
    // Arrange
    Wormhole<Integer> wormhole = new Wormhole<>(3);
    int maxKeyLength = 8;
    String[] keys = new String[] { "sngt", "twfyxjcb", "saep", "pc", "d", "nvko", "zrcsouzv", "nc", "o", "zrdxyxxo", "eiekorn", "vqprdfo", "rbgpfo", "gtbpfes" };
    Map<String, Integer> expected = new HashMap<>();

    // Act
    for (String key : keys) {
      // TODO: NPE occurs when the key length is 0.
      int value = ThreadLocalRandom.current().nextInt();
      expected.put(key, value);
      try {
        wormhole.put(key, value);
      }
      catch (Exception e) {
        System.err.printf("Failed to put. Key: %s, Value: %d, Wormhole:%s%n", key, value, wormhole);
        throw e;
      }
    }

    // Assert
    for (Map.Entry<String, Integer> entry : expected.entrySet()) {
      if (entry.getValue().equals(wormhole.get(entry.getKey()))) {
      }
      else {
        System.out.printf("key=%s, value=%d%n", entry.getKey(), entry.getValue());
        Integer i = wormhole.get(entry.getKey());
        System.out.println(">>>>>>>>>>>>>>>>>> " + i);
      }
    }
  }
}