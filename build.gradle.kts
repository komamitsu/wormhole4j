plugins {
    id("java")
}

group = "org.example"
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

tasks.test {
    useJUnitPlatform()
}