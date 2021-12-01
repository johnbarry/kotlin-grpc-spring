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
    implementation( "io.projectreactor.kafka:reactor-kafka:1.3.7")
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

tasks.register<Copy>("copyDistro") {
    onlyIf { project.hasProperty("LOCAL_TEST_DIR") }
    from(zipTree("$buildDir/distributions/server.zip"))
    into(file(findProperty("LOCAL_TEST_DIR").toString().drop(1).dropLast(1)))
}



tasks.named("startScripts") {
    dependsOn(helloWorldServerStartScripts)
}
