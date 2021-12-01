plugins {
    application
    kotlin("jvm")
}

dependencies {
    implementation(project(":stub"))
    runtimeOnly("io.grpc:grpc-netty:${rootProject.ext["grpcVersion"]}")
}

tasks.register<JavaExec>("HelloWorldClient") {
    dependsOn("classes")
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("io.grpc.examples.helloworld.HelloWorldClientKt")
}


val helloWorldClientStartScripts = tasks.register<CreateStartScripts>("helloWorldClientStartScripts") {
    mainClass.set("io.grpc.examples.helloworld.HelloWorldClientKt")
    applicationName = "hello-world-client"
    outputDir = tasks.named<CreateStartScripts>("startScripts").get().outputDir
    classpath = tasks.named<CreateStartScripts>("startScripts").get().classpath
}

tasks.named("startScripts") {
    dependsOn(helloWorldClientStartScripts)
}

tasks.register<Copy>("copyDistro") {
    onlyIf { project.hasProperty("LOCAL_TEST_DIR") }
    from(zipTree("$buildDir/distributions/client.zip"))
    into(file(findProperty("LOCAL_TEST_DIR").toString().drop(1).dropLast(1)))
}
