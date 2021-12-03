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

