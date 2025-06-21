package org.komamitsu.wormhole;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.assertj.core.api.Assertions.assertThat;

class WormholeTest {
  @Nested
  class Put {
    @Test
    void givenSingleRecord_ShouldStoreIt() {
      // Arrange
      Wormhole<String> wormhole = new Wormhole<>(4);

      // Act
      wormhole.put("James", "semaj");

      // Assert
      String value = wormhole.get("James");
      assertThat(value).isEqualTo("semaj");
    }
  }
}