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

import static org.komamitsu.wormhole4j.jmh.Constants.*;
import static org.komamitsu.wormhole4j.jmh.Utils.*;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.komamitsu.wormhole4j.KeyValue;
import org.komamitsu.wormhole4j.WormholeForStringKey;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkWormholeForStringKey {

  @State(Scope.Thread)
  public static class EmptyState {
    WormholeForStringKey<Integer> wormhole;

    @Setup(Level.Iteration)
    public void setup(StringKeysState data) {
      wormhole = new WormholeForStringKey<>();
    }
  }

  @Benchmark
  @OperationsPerInvocation(INSERT_OPS_COUNT)
  public void benchmarkInsert(StringKeysState keysState, EmptyState emptyState) {
    iterateRecordCountTimes(i -> emptyState.wormhole.put(keysState.keys.get(i), i));
  }

  @State(Scope.Thread)
  public static class FullState {
    WormholeForStringKey<Integer> wormhole;

    @Setup(Level.Iteration)
    public void setup(StringKeysState data) {
      wormhole = new WormholeForStringKey<>();
      for (String key : data.keys) {
        wormhole.put(key, randomInt());
      }
    }
  }

  @Benchmark
  @OperationsPerInvocation(GET_OPS_COUNT)
  public void benchmarkGet(StringKeysState keysState, FullState fullState, Blackhole blackhole) {
    iterateRecordCountTimes(i -> blackhole.consume(fullState.wormhole.get(keysState.keys.get(i))));
  }

  @Benchmark
  @OperationsPerInvocation(SCAN_OPS_COUNT)
  public void benchmarkScan(StringKeysState keysState, FullState fullState) {
    Function<KeyValue<String, Integer>, Boolean> function = (kv) -> true;
    iterateRecordCountTimesWithIndexRange(
        keysState, (k1, k2) -> fullState.wormhole.scan(k1, k2, true, function));
  }
}
