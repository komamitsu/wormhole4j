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

import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import java.util.concurrent.TimeUnit;
import org.komamitsu.wormhole4j.jmh.state.IntKeysState;
import org.komamitsu.wormhole4j.jmh.state.KeysState;
import org.komamitsu.wormhole4j.jmh.state.LongKeysState;
import org.komamitsu.wormhole4j.jmh.state.StringKeysState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public abstract class AVLTreeBenchmark<K extends Comparable<K>> {

  protected abstract static class FullState<K extends Comparable<K>> {
    Object2ObjectSortedMap<K, Integer> map;

    protected void setup(KeysState<K> keysState) {
      map = new Object2ObjectAVLTreeMap<>();
      for (K key : keysState.keys) {
        map.put(key, randomInt());
      }
    }
  }

  protected void execGet(KeysState<K> keysState, FullState<K> fullState, Blackhole blackhole) {
    blackhole.consume(fullState.map.get(keysState.getRandomKey()));
  }

  protected void execPut(KeysState<K> keysState, FullState<K> fullState) {
    fullState.map.put(keysState.getRandomKey(), 42);
  }

  protected void execScan(KeysState<K> keysState, FullState<K> fullState, Blackhole blackhole) {
    keysState.withRandomKeyRange(
        (startKey, endKey) ->
            fullState.map.subMap(startKey, endKey).forEach((key, value) -> blackhole.consume(key)));
  }

  public static class ForIntKey extends AVLTreeBenchmark<Integer> {
    @State(Scope.Benchmark)
    public static class FullState extends AVLTreeBenchmark.FullState<Integer> {
      @Setup(Level.Trial)
      public void setup(IntKeysState keysState) {
        super.setup(keysState);
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

  public static class ForLongKey extends AVLTreeBenchmark<Long> {
    @State(Scope.Benchmark)
    public static class FullState extends AVLTreeBenchmark.FullState<Long> {
      @Setup(Level.Trial)
      public void setup(LongKeysState keysState) {
        super.setup(keysState);
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

  public static class ForStringKey extends AVLTreeBenchmark<String> {
    @State(Scope.Benchmark)
    public static class FullState extends AVLTreeBenchmark.FullState<String> {
      @Setup(Level.Trial)
      public void setup(StringKeysState keysState) {
        super.setup(keysState);
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
