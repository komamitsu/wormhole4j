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
public abstract class ConcurrentWormholeMultiThreadBenchmark<K extends Comparable<K>> {

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

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkPut(
        IntKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkGet(
        IntKeysState keysState, FullState fullState, Blackhole blackhole, ThreadState threadState) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkPut(
        IntKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkScan(
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
      public void unregister(ForIntKey.FullState state) {
        state.map.unregisterThread();
      }
    }

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkPut(
        LongKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkGet(
        LongKeysState keysState,
        FullState fullState,
        Blackhole blackhole,
        ThreadState threadState) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkPut(
        LongKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkScan(
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
      public void unregister(ForIntKey.FullState state) {
        state.map.unregisterThread();
      }
    }

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkPut(
        StringKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndGet")
    @GroupThreads(4)
    @Benchmark
    public void putAndGetBenchmarkGet(
        StringKeysState keysState,
        FullState fullState,
        Blackhole blackhole,
        ThreadState threadState) {
      execGet(keysState, fullState, blackhole);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkPut(
        StringKeysState keysState, FullState fullState, ThreadState threadState) {
      execPut(keysState, fullState);
    }

    @Group("PutAndScan")
    @GroupThreads(4)
    @Benchmark
    public void putAndScanBenchmarkScan(
        StringKeysState keysState,
        FullState fullState,
        Blackhole blackhole,
        ThreadState threadState) {
      execScan(keysState, fullState, blackhole);
    }
  }
}
