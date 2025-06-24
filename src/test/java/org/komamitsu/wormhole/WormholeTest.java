package org.komamitsu.wormhole;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

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
  }
}