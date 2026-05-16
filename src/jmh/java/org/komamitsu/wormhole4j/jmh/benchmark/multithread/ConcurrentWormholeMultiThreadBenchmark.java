/*
 * Copyright 2026 Mitsunori Komatsu
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

package org.komamitsu.wormhole4j.jmh.benchmark.multithread;

import static org.komamitsu.wormhole4j.jmh.Utils.randomInt;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import org.komamitsu.wormhole4j.*;
import org.komamitsu.wormhole4j.jmh.Constants;
import org.komamitsu.wormhole4j.jmh.state.IntKeysState;
import org.komamitsu.wormhole4j.jmh.state.KeysState;
import org.komamitsu.wormhole4j.jmh.state.LongKeysState;
import org.komamitsu.wormhole4j.jmh.state.StringKeysState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

public abstract class ConcurrentWormholeMultiThreadBenchmark<K extends Comparable<K>>
    extends MultiThreadBenchmark {

  protected abstract static class FullState<K extends Comparable<K>> {
    Wormhole<K, Integer> map;

    protected void setup(Wormhole<K, Integer> wormhole, KeysState<K> keysState) {
      map = wormhole;
      map.registerThread();
      for (K key : keysState.keys) {
        map.put(key, randomInt());
      }
      map.unregisterThread();
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

  public static class ForIntKey extends ConcurrentWormholeMultiThreadBenchmark<Integer> {
    @State(Scope.Benchmark)
    public static class FullState
        extends ConcurrentWormholeMultiThreadBenchmark.FullState<Integer> {
      @Setup(Level.Trial)
      public void setup(IntKeysState keysState) {
        super.setup(
            new WormholeBuilder.ForIntKey<Integer>().setConcurrent(true).build(), keysState);
      }
    }

    @State(Scope.Thread)
    public static class ThreadState {
      @Setup(Level.Trial)
      public void register(FullState state) {
        state.map.registerThread();
      }

      @TearDown(Level.Trial)
      public void unregister(FullState state) {
        state.map.unregisterThread();
      }
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkUpdate(
        IntKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkGet(
        IntKeysState keysState, FullState fullState, Blackhole blackhole, ThreadState threadState) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("UpdateAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkUpdate(
        IntKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("UpdateAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkScan(
        IntKeysState keysState, FullState fullState, Blackhole blackhole, ThreadState threadState) {
      execScan(keysState, fullState, blackhole);
    }
  }

  public static class ForLongKey extends ConcurrentWormholeMultiThreadBenchmark<Long> {
    @State(Scope.Benchmark)
    public static class FullState extends ConcurrentWormholeMultiThreadBenchmark.FullState<Long> {
      @Setup(Level.Trial)
      public void setup(LongKeysState keysState) {
        super.setup(
            new WormholeBuilder.ForLongKey<Integer>().setConcurrent(true).build(), keysState);
      }
    }

    @State(Scope.Thread)
    public static class ThreadState {
      @Setup(Level.Trial)
      public void register(FullState state) {
        state.map.registerThread();
      }

      @TearDown(Level.Trial)
      public void unregister(FullState state) {
        state.map.unregisterThread();
      }
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkUpdate(
        LongKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkGet(
        LongKeysState keysState,
        FullState fullState,
        Blackhole blackhole,
        ThreadState threadState) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("UpdateAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkUpdate(
        LongKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("UpdateAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkScan(
        LongKeysState keysState,
        FullState fullState,
        Blackhole blackhole,
        ThreadState threadState) {
      execScan(keysState, fullState, blackhole);
    }
  }

  public static class ForStringKey extends ConcurrentWormholeMultiThreadBenchmark<String> {
    @State(Scope.Benchmark)
    public static class FullState extends ConcurrentWormholeMultiThreadBenchmark.FullState<String> {
      @Setup(Level.Trial)
      public void setup(StringKeysState keysState) {
        super.setup(
            new WormholeBuilder.ForStringKey<Integer>().setConcurrent(true).build(), keysState);
      }
    }

    @State(Scope.Thread)
    public static class ThreadState {
      @Setup(Level.Trial)
      public void register(FullState state) {
        state.map.registerThread();
      }

      @TearDown(Level.Trial)
      public void unregister(FullState state) {
        state.map.unregisterThread();
      }
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkUpdate(
        StringKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("UpdateAndGet")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndGetBenchmarkGet(
        StringKeysState keysState,
        FullState fullState,
        Blackhole blackhole,
        ThreadState threadState) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("UpdateAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkUpdate(
        StringKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("UpdateAndScan")
    @GroupThreads(Constants.GROUP_THREADS)
    @Benchmark
    public void updateAndScanBenchmarkScan(
        StringKeysState keysState,
        FullState fullState,
        Blackhole blackhole,
        ThreadState threadState) {
      execScan(keysState, fullState, blackhole);
    }
  }
}
