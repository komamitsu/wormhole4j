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

plugins {
    id("java")
    id("com.diffplug.spotless") version "6.13.0"
}

group = "org.komamitsu"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.2")
    testImplementation("io.github.myui:btree4j:0.9.1")
    testImplementation("it.unimi.dsi:fastutil:8.5.16")
    testImplementation("org.mapdb:mapdb:3.0.9")
}

sourceSets {
    create("benchmark") {
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    }
}

val benchmarkImplementation: Configuration by configurations.getting {
    extendsFrom(configurations.testImplementation.get())
}
val benchmarkRuntimeOnly: Configuration by configurations.getting {
    extendsFrom(configurations.testRuntimeOnly.get())
}

tasks.test {
    useJUnitPlatform()
}

val benchmark = task<Test>("benchmark") {
    description = "Runs benchmark."
    group = "verification"
    testClassesDirs = sourceSets["benchmark"].output.classesDirs
    classpath = sourceSets["benchmark"].runtimeClasspath
    useJUnitPlatform()
}

spotless {
    java {
        googleJavaFormat()
        importOrder()
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
}