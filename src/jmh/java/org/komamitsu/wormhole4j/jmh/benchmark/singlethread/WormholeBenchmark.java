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

package org.komamitsu.wormhole4j.jmh.benchmark.singlethread;

import static org.komamitsu.wormhole4j.jmh.Utils.randomInt;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.komamitsu.wormhole4j.*;
import org.komamitsu.wormhole4j.jmh.state.IntKeysState;
import org.komamitsu.wormhole4j.jmh.state.KeysState;
import org.komamitsu.wormhole4j.jmh.state.LongKeysState;
import org.komamitsu.wormhole4j.jmh.state.StringKeysState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public abstract class WormholeBenchmark<K extends Comparable<K>> {

  protected abstract static class FullState<K extends Comparable<K>> {
    Wormhole<K, Integer> map;

    protected void setup(Wormhole<K, Integer> wormhole, KeysState<K> keysState) {
      map = wormhole;
      for (K key : keysState.keys) {
        map.put(key, randomInt());
      }
    }
  }

  protected void execGet(KeysState<K> keysState, FullState<K> fullState, Blackhole blackhole) {
    blackhole.consume(fullState.map.get(keysState.getRandomKey()));
  }

  protected void execPut(KeysState<K> keysState, FullState<K> fullState) {
    fullState.map.put(keysState.getRandomKey(), ThreadLocalRandom.current().nextInt());
  }

  protected void execScan(KeysState<K> keysState, FullState<K> fullState, Blackhole blackhole) {
    BiFunction<K, Integer, Boolean> function =
        (k, v) -> {
          blackhole.consume(k);
          return true;
        };
    keysState.withRandomKeyRange(
        (startKey, endKey) -> fullState.map.scan(startKey, endKey, true, function));
  }

  public static class ForIntKey extends WormholeBenchmark<Integer> {
    @State(Scope.Benchmark)
    public static class FullState extends WormholeBenchmark.FullState<Integer> {
      @Setup(Level.Trial)
      public void setup(IntKeysState keysState) {
        super.setup(new WormholeBuilder.ForIntKey<Integer>().build(), keysState);
      }
    }

    @Benchmark
    public void benchmarkGet(IntKeysState keysState, FullState fullState, Blackhole blackhole) {
      execGet(keysState, fullState, blackhole);
    }

    @Benchmark
    public void benchmarkPut(IntKeysState keysState, FullState fullState) {
      execPut(keysState, fullState);
    }

    @Benchmark
    public void benchmarkScan(IntKeysState keysState, FullState fullState, Blackhole blackhole) {
      execScan(keysState, fullState, blackhole);
    }
  }

  public static class ForLongKey extends WormholeBenchmark<Long> {
    @State(Scope.Benchmark)
    public static class FullState extends WormholeBenchmark.FullState<Long> {
      @Setup(Level.Trial)
      public void setup(LongKeysState keysState) {
        super.setup(new WormholeBuilder.ForLongKey<Integer>().build(), keysState);
      }
    }

    @Benchmark
    public void benchmarkGet(LongKeysState keysState, FullState fullState, Blackhole blackhole) {
      execGet(keysState, fullState, blackhole);
    }

    @Benchmark
    public void benchmarkPut(LongKeysState keysState, FullState fullState) {
      execPut(keysState, fullState);
    }

    @Benchmark
    public void benchmarkScan(LongKeysState keysState, FullState fullState, Blackhole blackhole) {
      execScan(keysState, fullState, blackhole);
    }
  }

  public static class ForStringKey extends WormholeBenchmark<String> {
    @State(Scope.Benchmark)
    public static class FullState extends WormholeBenchmark.FullState<String> {
      @Setup(Level.Trial)
      public void setup(StringKeysState keysState) {
        super.setup(new WormholeBuilder.ForStringKey<Integer>().build(), keysState);
      }
    }

    @Benchmark
    public void benchmarkGet(StringKeysState keysState, FullState fullState, Blackhole blackhole) {
      execGet(keysState, fullState, blackhole);
    }

    @Benchmark
    public void benchmarkPut(StringKeysState keysState, FullState fullState) {
      execPut(keysState, fullState);
    }

    @Benchmark
    public void benchmarkScan(StringKeysState keysState, FullState fullState, Blackhole blackhole) {
      execScan(keysState, fullState, blackhole);
    }
  }
}
