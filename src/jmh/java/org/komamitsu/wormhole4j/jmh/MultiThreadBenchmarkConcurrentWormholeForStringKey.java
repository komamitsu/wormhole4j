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

package org.komamitsu.wormhole4j.jmh;

import static org.komamitsu.wormhole4j.jmh.Utils.randomInt;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.komamitsu.wormhole4j.WormholeForStringKey;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MultiThreadBenchmarkConcurrentWormholeForStringKey {

  @State(Scope.Group)
  public static class FullState {
    WormholeForStringKey<Integer> map;
    int counter;

    @Setup(Level.Trial)
    public void setup(StringKeysState data) {
      map = new WormholeForStringKey.Builder<Integer>().setConcurrent(true).build();
      for (String key : data.keys) {
        map.put(key, randomInt());
      }
    }
  }

  @Group("PutAndGet")
  @GroupThreads(8)
  @Benchmark
  public void putAndGetBenchmarkPut(StringKeysState keysState, FullState fullState) {
    fullState.map.put(keysState.getRandomKey(), 42);
  }

  @Group("PutAndGet")
  @GroupThreads(8)
  @Benchmark
  public void putAndGetBenchmarkGet(
      StringKeysState keysState, FullState fullState, Blackhole blackhole) {
    blackhole.consume(fullState.map.get(keysState.getRandomKey()));
  }

  @Group("PutAndScan")
  @GroupThreads(8)
  @Benchmark
  public void putAndScanBenchmarkPut(StringKeysState keysState, FullState fullState) {
    fullState.map.put(keysState.getRandomKey(), 42);
  }

  @Group("PutAndScan")
  @GroupThreads(8)
  @Benchmark
  public void putAndScanBenchmarkScan(
      StringKeysState keysState, FullState fullState, Blackhole blackhole) {
    BiFunction<String, Integer, Boolean> function =
        (k, v) -> {
          fullState.counter++;
          return true;
        };
    keysState.withRandomKeyRange(
        (startKey, endKey) -> fullState.map.scan(startKey, endKey, true, function));
    blackhole.consume(fullState.counter);
  }
}
