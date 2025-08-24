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
import org.jreleaser.model.Active

plugins {
    `java-library`
    `maven-publish`
    id("org.jreleaser") version "1.19.0"
    id("com.diffplug.spotless") version "6.13.0"
}

group = "org.komamitsu"
version = "0.1.0"

val projectName = "Wormhole4j"
val projectDescription = "High-performance ordered in-memory index for Java"
val projectUrl = "https://github.com/komamitsu/wormhole4j"
val projectAuthor = "Mitsunori Komatsu"

repositories {
    mavenCentral()
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

dependencies {
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.2")
    benchmarkImplementation("io.github.myui:btree4j:0.9.1")
    benchmarkImplementation("it.unimi.dsi:fastutil:8.5.16")
    benchmarkImplementation("org.mapdb:mapdb:3.0.9")
}

java {
    withSourcesJar()
    withJavadocJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
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

tasks.test {
    useJUnitPlatform()
}

val benchmark = task<Test>("benchmark") {
    description = "Runs benchmark."
    group = "verification"
    testClassesDirs = sourceSets["benchmark"].output.classesDirs
    classpath = sourceSets["benchmark"].runtimeClasspath
    useJUnitPlatform()
    outputs.upToDateWhen { false }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set(projectName)
                description.set(projectDescription)
                url.set(projectUrl)
                licenses {
                    license {
                        name.set("Apache License 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.html")
                    }
                }
                developers {
                    developer {
                        id.set("komamitsu")
                        name.set(projectAuthor)
                        email.set("komamitsu@gmail.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/komamitsu/wormhole4j.git")
                    developerConnection.set("scm:git:ssh://github.com/komamitsu/wormhole4j.git")
                    url.set(projectUrl)
                }
            }
        }
    }
}

jreleaser {
    gitRootSearch = true

    project {
        inceptionYear = "2025"
    }

    signing {
        active = Active.ALWAYS
        armored = true
    }

    deploy {
        maven {
            mavenCentral.create("sonatype") {
                active = Active.ALWAYS
                url = "https://central.sonatype.com/api/v1/publisher"
                applyMavenCentralRules = true
                stagingRepositories.add("${layout.buildDirectory.get()}/staging-deploy")
            }
        }
    }
}
