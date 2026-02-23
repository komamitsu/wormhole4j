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

import kotlinx.atomicfu.locks.synchronized
import org.jetbrains.lincheck.*
import org.jetbrains.lincheck.datastructures.*
import org.junit.*
import org.junit.jupiter.api.Test
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
class Counter {
    @Volatile
    private var value = 0

    fun inc() {
//        synchronized(this) {
//            ++value
//        }
    }

    fun get() = value
}

class BasicCounterTest {
    private val c = Counter() // Initial state

    // Operations on the Counter
    @Operation
    fun inc() = c.inc()

    @Operation
    fun get() = c.get()

//    fun stressTest() = StressOptions().check(this::class) // The magic button
    @Test // JUnit
    fun modelCheckingTest() = ModelCheckingOptions().check(this::class)
}