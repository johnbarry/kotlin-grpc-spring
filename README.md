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
// numbers output as strings - so this is overly simplistic
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
Server backpressure demo: 10000
Server backpressure demo: 20000
Server backpressure demo: 30000
Server backpressure demo: 40000
Server backpressure demo: 50000
Server backpressure demo: 60000
Server backpressure demo: 70000
Server backpressure demo: 80000
Server backpressure demo: 90000
Server backpressure demo: 100000
Server backpressure demo: 110000
Server backpressure demo: 120000
Client backpressure demo: 10000
Client backpressure demo: 20000
Client backpressure demo: 30000
Client backpressure demo: 40000
Client backpressure demo: 50000
Client backpressure demo: 60000
Server backpressure demo: 130000
Server backpressure demo: 140000
Server backpressure demo: 150000
Server backpressure demo: 160000
Server backpressure demo: 170000
Server backpressure demo: 180000
Client backpressure demo: 70000
Client backpressure demo: 80000
Client backpressure demo: 90000
Client backpressure demo: 100000
Client backpressure demo: 110000
Server backpressure demo: 190000
Server backpressure demo: 200000
```

### Example - large message size

```text
Server backpressure demo: 100
Server backpressure demo: 200
Server backpressure demo: 300
Server backpressure demo: 400
Client backpressure demo: 100
Client backpressure demo: 200
Server backpressure demo: 500
Client backpressure demo: 300
Client backpressure demo: 400
Server backpressure demo: 600
Client backpressure demo: 500
Server backpressure demo: 700
Client backpressure demo: 600
Server backpressure demo: 800
Client backpressure demo: 700
Client backpressure demo: 800
Server backpressure demo: 900
Client backpressure demo: 900
Server backpressure demo: 1000
Client backpressure demo: 1000
Server backpressure demo: 1100
Client backpressure demo: 1100
Server backpressure demo: 1200
Client backpressure demo: 1200
Server backpressure demo: 1300
Server backpressure demo: 1400
Client backpressure demo: 1300
Client backpressure demo: 1400
Server backpressure demo: 1500
Client backpressure demo: 1500
Server backpressure demo: 1600
Client backpressure demo: 1600
Server backpressure demo: 1700
Client backpressure demo: 1700
Server backpressure demo: 1800
Client backpressure demo: 1800
Server backpressure demo: 1900
Client backpressure demo: 1900
Server backpressure demo: 2000
```
