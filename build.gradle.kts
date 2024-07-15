plugins {
    java
    id("org.springframework.boot") version "3.3.1"
    id("io.spring.dependency-management") version "1.1.5"
    id("io.freefair.lombok") version "8.6"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")

    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.apache.kafka:kafka-streams")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.wiremock:wiremock-standalone:3.8.0")
    testImplementation("org.awaitility:awaitility:4.2.1")

    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.awaitility:awaitility:4.2.1")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
