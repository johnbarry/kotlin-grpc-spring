# GRPC, Kafka, Spring Reactor Playground

## Objectives

- gRPC demo (from Kotlin examples)
- Use of Kafka for events/commands
- Use of Spring Reactor with Kafka 
- Spring Reactor Flux vs gRPC Flow


## Directory layout (from Kotlin gRPC examples)

The example sources are organized into the following top-level folders:

- [protos](protos) `.proto` files (shared across examples)
- [stub](stub) regular Java & Kotlin stub artifacts from [protos](protos)
- [client](client): Kotlin clients based on regular [stub](stub) artifacts
- [server](server): Kotlin servers based on regular [stub](stub) artifacts

## Server (Kafka) setup

You will need to set KAFKA_SERVER environment variable to Kafka address before running.

e.g. KAFKA_SERVER=localhost:9092

## gRPC for streaming, callbacks and combined

gRPC allows a single API to combine streaming and regular request/response callbacks..

```protobuf
service FriendService {
  rpc peopleChangeEvents (Empty) returns (stream PersonChangeEvent) {}
  rpc listPeopleChanges (Empty) returns (stream PersonChange) {}
  rpc changeCallback(PersonChangeEvent) returns (PersonChange) {}
}
```

### Separate stream and callback

- _peopleChangeEvents()_ gets a stream of events where each event is a small notification that the person record has changed
- _changeCallBack()_ takes the event and gets the full details of the record

This is using the server-side streaming feature of gRPC.

### Combined stream and callback

- _listPeopleChanges()_ combines the 2 within the service (server) code returning full details of all changed records

This is using the server-side streaming feature of gRPC.


## Run REST server and gRPC server

Implemented a REST endpoint to call the underlying data stream (_Flux_) used by the gRPC server but convert the protobuf results to JSON.

### Running as both web server and gRPC server

Main class extends _ApplicationRunner_ and startup is _REACTIVE_ web type 

```kotlin
open class HelloWorldServer : ApplicationRunner {
    override fun run(args: ApplicationArguments?) {
        val server = HelloWorldServer()
        server.start()
    }
}

fun main(args: Array<String>) {
    val app = SpringApplication(HelloWorldServer::class.java)
    app.webApplicationType = WebApplicationType.REACTIVE
    app.run(*args)
}
```

### REST built on gRPC

A REST service could call the gRPC service directly, wrapping the gRPC service as REST ...

```kotlin
class PersonHandler {
    private val service = FriendService()

    fun list(request: ServerRequest): JsonResponse =
        // the gRPC service call ...
        service.listPeople().asRestResponse()

    fun get(request: ServerRequest): JsonResponse =
        runBlocking {
            // the gRPC service call ...
            service.getPerson( personId {  id = request.pathVariable("id").toLong()  })
                    .asRestResponse()
        }

```

... which is routed as follows using Kotlin DSL: 

```kotlin
class PersonRouter(private val handler: PersonHandler) {

    fun theRouter() = router {
        accept(MediaType.APPLICATION_JSON).nest {
            GET("/people").invoke(handler::list)
            GET("/person/{id}").invoke(handler::get)
        }
    }
}
```

### JSON handler

Can use generic JSON handling across protobuf classes:

```kotlin
// Kotlin extension function
fun GeneratedMessageV3.asJson(): String =
    JsonFormat.printer().print(this)
        .replace("\\n".toRegex(), "")
        .replace("\\r".toRegex(), "")

fun Flux<GeneratedMessageV3>.asRestResponse(): JsonResponse =
    ServerResponse.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(
            BodyInserters.fromPublisher(
                this.map { it.asJson() + "\n" },
                String::class.java
            )
        )

fun Flow<GeneratedMessageV3>.asRestResponse(): JsonResponse =
    asFlux().asRestResponse()

fun GeneratedMessageV3.asRestResponse(): JsonResponse =
    Flux.just(this).asRestResponse()

}
```

### Example

http://localhost:8080/people

```json lines
{  "forename": "Harry",  "surname": "A-A"}
{  "forename": "Harry",  "surname": "A-B",  "id": "1"}
{  "forename": "Harry",  "surname": "A-C",  "id": "2"}
{  "forename": "Harry",  "surname": "A-D",  "id": "3"}
{  "forename": "Harry",  "surname": "A-E",  "id": "4"}
```

http://localhost:8080/person/4

```json lines
{  "forename": "Harry",  "surname": "A-E",  "id": "4"}
```

## Backpressure demo

HTTP/2, gRPC and Spring Reactor all have backpressure so we can stream server thru to client:

### Server
```kotlin
Flux.range(1, request.number)
   .map{ aNumber {
        number = it
        (request.addFiller)
            (1..it).forEach { _ -> filler.add("BlahBlahBlah") } }}
    .doOnNext {
        ...
        log.info("Server backpressure demo: $n")
        ...  }
    .asFlow()
```

### Client

```kotlin
stub.backPressureDemo ( backPressureDemoRequest {
    number = countTo
    addFiller = withFiller})
    .collect {
        ...
        println("Client backpressure demo: $n")
        ...  }
```

### Example - small message size

```text
=========> Small message no client delay
2021-12-03 16:57:58.234  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 10000
2021-12-03 16:57:58.361  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 20000
2021-12-03 16:57:58.478  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 30000
2021-12-03 16:57:58.601  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 40000
2021-12-03 16:57:58.733  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 50000
2021-12-03 16:57:58.829  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 60000
2021-12-03 16:57:58.916  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 70000
2021-12-03 16:57:59.011  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 80000
2021-12-03 16:57:59.102  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 90000
2021-12-03 16:57:59.162  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 100000
2021-12-03 16:57:59.217  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 110000
2021-12-03 16:57:59.277  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 120000
Client backpressure demo: 10000
Client backpressure demo: 20000
Client backpressure demo: 30000
Client backpressure demo: 40000
Client backpressure demo: 50000
Client backpressure demo: 60000
2021-12-03 16:58:04.021  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 130000
2021-12-03 16:58:04.066  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 140000
2021-12-03 16:58:04.112  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 150000
2021-12-03 16:58:04.147  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 160000
2021-12-03 16:58:04.174  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 170000
2021-12-03 16:58:04.199  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 180000
Client backpressure demo: 70000
Client backpressure demo: 80000
Client backpressure demo: 90000
Client backpressure demo: 100000
Client backpressure demo: 110000
2021-12-03 16:58:08.235  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 190000
2021-12-03 16:58:08.257  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 200000
```

### Example - large message size

```text
=========> **Large message 10ms client delay**
2021-12-03 16:58:23.562  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 100
2021-12-03 16:58:23.596  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 200
2021-12-03 16:58:23.621  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 300
2021-12-03 16:58:23.672  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 400
Client backpressure demo: 100
Client backpressure demo: 200
2021-12-03 16:58:26.434  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 500
Client backpressure demo: 300
Client backpressure demo: 400
2021-12-03 16:58:28.501  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 600
Client backpressure demo: 500
2021-12-03 16:58:29.264  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 700
Client backpressure demo: 600
2021-12-03 16:58:30.540  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 800
Client backpressure demo: 700
Client backpressure demo: 800
2021-12-03 16:58:32.095  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 900
Client backpressure demo: 900
2021-12-03 16:58:33.000  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1000
Client backpressure demo: 1000
2021-12-03 16:58:34.215  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1100
Client backpressure demo: 1100
2021-12-03 16:58:34.948  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1200
Client backpressure demo: 1200
2021-12-03 16:58:36.289  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1300
2021-12-03 16:58:36.919  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1400
Client backpressure demo: 1300
Client backpressure demo: 1400
2021-12-03 16:58:38.366  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1500
Client backpressure demo: 1500
2021-12-03 16:58:39.169  INFO 15367 --- [atcher-worker-1] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1600
Client backpressure demo: 1600
2021-12-03 16:58:40.429  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1700
Client backpressure demo: 1700
2021-12-03 16:58:41.822  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1800
Client backpressure demo: 1800
2021-12-03 16:58:42.484  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 1900
Client backpressure demo: 1900
2021-12-03 16:58:43.749  INFO 15367 --- [atcher-worker-2] i.g.e.helloworld.HelloWorldServer        : Server backpressure demo: 2000
```
