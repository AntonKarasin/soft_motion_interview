plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.groovy:groovy-xml:5.0.3")
    implementation("org.postgresql:postgresql:42.7.8")
    implementation("org.json:json:20231013")
}

tasks.withType<JavaExec> {
    systemProperty("file.encoding", "UTF-8")
    standardInput = System.`in`
}

tasks.test {
    useJUnitPlatform()
}