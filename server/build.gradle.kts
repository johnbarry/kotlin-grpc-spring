import org.jetbrains.kotlin.ir.backend.js.compile

plugins {
    application
    kotlin("jvm")
    id ("org.springframework.boot") version "2.5.2"
    id ("io.spring.dependency-management") version "1.0.11.RELEASE"
}

dependencies {
    implementation(project(":stub"))
    implementation ("org.springframework.boot:spring-boot-starter-actuator")
    implementation ("org.springframework.cloud:spring-cloud-starter-stream-kafka")
    runtimeOnly("io.grpc:grpc-netty:${rootProject.ext["grpcVersion"]}")
}

dependencyManagement {
    imports {
        mavenBom ("org.springframework.cloud:spring-cloud-dependencies:${rootProject.ext["springCloudVersion"]}")
    }
}
springBoot {
 setProperty("mainClassName", "io.grpc.examples.helloworld.HelloWorldServer")
}

tasks.register<JavaExec>("HelloWorldServer") {
    dependsOn("classes")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("io.grpc.examples.helloworld.HelloWorldServerKt")
}

val helloWorldServerStartScripts = tasks.register<CreateStartScripts>("helloWorldServerStartScripts") {
    mainClass.set("io.grpc.examples.helloworld.HelloWorldServerKt")
    applicationName = "hello-world-server"
    outputDir = tasks.named<CreateStartScripts>("startScripts").get().outputDir
    classpath = tasks.named<CreateStartScripts>("startScripts").get().classpath
}


tasks.named("startScripts") {
    dependsOn(helloWorldServerStartScripts)
}
