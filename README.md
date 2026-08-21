# AWS Glue Schema Registry Library

[![JVM Library](https://github.com/mobsuccess-devops/aws-glue-schema-registry/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/mobsuccess-devops/aws-glue-schema-registry/actions/workflows/ci.yml)
[![Apache 2 License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](http://aws.amazon.com/apache-2-0/)
![JVM 17](https://img.shields.io/badge/JVM-17-blue.svg)

Mobsuccess fork of [`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry).

**AWS Glue Schema Registry** lets you centrally discover, control and evolve schemas while
ensuring produced data was validated against a registered schema. This library provides the
serializers and deserializers that plug into it.

## How this fork differs

The library behaviour is unchanged — the inherited test suite (2087 tests) passes in full,
and the first commit of this repository is the upstream source verbatim, so every deviation
is visible with `git diff eed1506`.

What changed is everything around the code:

|              | Upstream               | This fork                          |
| ------------ | ---------------------- | ---------------------------------- |
| Build        | Maven                  | Gradle 9.6.1, Kotlin DSL           |
| Languages    | Java + C#              | Java only, Kotlin port in progress |
| Distribution | Maven Central          | GitHub Packages                    |
| Group        | `software.amazon.glue` | `com.mobsuccess`                   |
| JVM target   | 8                      | 17                                 |

The C# binding and its native (GraalVM) layer were removed: without a binding, the native
layer had no consumer. Artifact names are unchanged. The group differs on purpose, so that
an artifact from this fork can never silently substitute for the Maven Central one.

Deviations are documented in [docs/portage.md](docs/portage.md); agent-facing notes live in
[AGENTS.md](AGENTS.md).

## Getting started

1. **Sign up for AWS** — see [AWS Account and Credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html)
   in the AWS SDK for Java Developer Guide.
2. **Sign up for AWS Glue Schema Registry** — see [Getting Started with Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-gs.html)
   in the AWS Glue Developer Guide.
3. **Minimum requirement** — JVM 17 or later.

## Features

- Records are serialized on the producer side and deserialized on the consumer side through
  `schema-registry-serde`.
- Three data formats: AVRO, JSON (via [JSON Schema](https://json-schema.org/) Draft04,
  Draft06 and Draft07) and Protocol Buffers (syntax 2 and 3).
- Kafka Streams, Kafka Connect and [Flink](https://ci.apache.org/projects/flink/flink-docs-release-1.14/docs/connectors/datastream/kafka/)
  integrations.
- Optional record compression to reduce message size.
- Built-in in-memory cache: the schema version id is cached producer-side, the schema itself
  consumer-side.
- Optional auto-registration of new schemas, with an evolution check on registration.
- Migration path from a third-party schema registry.

## Installation

Artifacts are published to GitHub Packages. Authentication is required even for reads — use a
token carrying the `read:packages` scope.

```kotlin
repositories {
    mavenCentral()
    maven {
        url = uri("https://maven.pkg.github.com/mobsuccess-devops/aws-glue-schema-registry")
        credentials {
            username = "_"
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("com.mobsuccess:schema-registry-serde:<version>")
}
```

Available artifacts:

| Artifact                                 | Purpose                                       |
| ---------------------------------------- | --------------------------------------------- |
| `schema-registry-common`                 | Glue client, cache, exceptions                |
| `schema-registry-serde`                  | Core SerDe (AVRO, JSON Schema, Protobuf)      |
| `schema-registry-serde-msk-iam`          | Uber-jar bundling the SerDe with MSK IAM auth |
| `schema-registry-kafkastreams-serde`     | Kafka Streams integration                     |
| `schema-registry-kafkaconnect-converter` | Kafka Connect AVRO converter                  |
| `jsonschema-kafkaconnect-converter`      | Kafka Connect JSON Schema converter           |
| `protobuf-kafkaconnect-converter`        | Kafka Connect Protobuf converter              |
| `schema-registry-flink-serde`            | Flink serialization schemas                   |

The four Kafka Connect / MSK IAM artifacts are shaded uber-jars, meant to be dropped onto a
Connect plugin path.

## Compatibility

The versions the artifacts are built and tested against. Everything except the JVM row comes
from `gradle/libs.versions.toml`, which is the single source of truth for the build.

| Component           | Version            | Notes                                                                                                                             |
| ------------------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| JVM                 | 17 or later        | Bytecode target is 17, so a JVM 8 or 11 runtime cannot load these artifacts.                                                      |
| Apache Kafka        | 3.9.x              | `kafka-clients`, `kafka-streams`, `connect-api`, `connect-json`. Shaded into the uber-jars: a consumer cannot override that copy. |
| Apache Avro         | 1.11.4             |                                                                                                                                   |
| Protocol Buffers    | 3.25.5             | `protobuf-java`; syntax 2 and 3.                                                                                                  |
| AWS SDK for Java v2 | 2.53.1             | Imported as a BOM, so the whole SDK moves together.                                                                               |
| MSK IAM auth        | 2.3.7              | `schema-registry-serde-msk-iam` only.                                                                                             |
| Apache Flink        | 1.12.2, Scala 2.11 | **Not recommended** — see below.                                                                                                  |

The Flink connector is carried over from upstream unchanged and is pinned to Flink 1.12.2 with
`flink-streaming-java_2.11`, a Scala 2.11 coordinate that Flink stopped publishing after 1.14.
It is kept so the fork stays behaviour-identical to its source, not because it is a reasonable
dependency to take today. New Flink work should use the Glue Schema Registry formats that ship
with [Apache Flink itself](https://github.com/apache/flink/tree/master/flink-formats).

## Migrating from the AWS artifact

Coming from `software.amazon.glue` on Maven Central, the swap is a coordinate change: the
artifactIds, the package names and the class names are all unchanged.

```diff
- implementation("software.amazon.glue:schema-registry-serde:1.1.x")
+ implementation("com.mobsuccess:schema-registry-serde:<version>")
```

Three things to check on the way:

1. **The repository.** GitHub Packages requires authentication even to read; add the
   repository block from [Installation](#installation) and a token carrying `read:packages`.
2. **The JVM.** Upstream targeted 8, this fork targets 17.
3. **Two behaviour deltas**, both deliberate:
   - A `@NonNull` violation raises a `NullPointerException` rather than the
     `IllegalArgumentException` upstream's `lombok.config` produced. A null argument is still
     rejected, at the same point; only the exception type differs. See
     [docs/portage.md](docs/portage.md).
   - Since **2.0.0**, the JSON deserializer no longer resolves a schema's `className` into a
     POJO by default and returns `JsonDataWithSchema` instead. Restoring the old behaviour
     takes both `jsonClassNameResolutionEnabled=true` and an explicit
     `jsonClassNameAllowlist`. See the [CHANGELOG](CHANGELOG.md) and
     [Deserializing JSON into a Java POJO](#deserializing-json-into-a-java-pojo-classname-resolution).

## Building from source

```bash
./gradlew clean build     # compile, run the 2087 tests, produce the jars
./gradlew test            # tests only
./gradlew assemble        # jars only
```

Gradle resolves a JVM 17 toolchain on its own; no local Maven or JDK pinning is needed.

## Testing

The `integration-tests` module requires real AWS resources. Its `*IntegrationTest` classes
are excluded from the unit run and are not executed in CI.

## Using the AWS Glue Schema Registry Library Serializer / Deserializer

The recommended way to use the AWS Glue Schema Registry Library is to consume the published artifact from GitHub Packages, as described in [Installation](#installation).

**Using AWS Glue Schema Registry with Amazon MSK** &mdash; To set-up Amazon Managed Streaming for Apache Kafka see
[Getting started with Amazon MSK.](https://docs.aws.amazon.com/msk/latest/developerguide/getting-started.html)

### Gradle dependency

```kotlin
implementation("com.mobsuccess:schema-registry-serde:<version>")
```

### Code Example

#### Producer for Kafka with AVRO format

```kotlin
properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = GlueSchemaRegistryKafkaSerializer::class.java.name
properties[AWSSchemaRegistryConstants.DATA_FORMAT] = DataFormat.AVRO.name
properties[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
properties[AWSSchemaRegistryConstants.REGISTRY_NAME] = "my-registry"
properties[AWSSchemaRegistryConstants.SCHEMA_NAME] = "my-schema"

val paymentSchema = Schema.Parser().parse(File("src/main/resources/avro/com/tutorial/Payment.avsc"))

val musical = GenericData.Record(paymentSchema)
musical.put("id", "entertainment_2")
musical.put("amount", 105.0)

val misc = listOf<GenericRecord>(musical)

try {
    KafkaProducer<String, GenericRecord>(properties).use { producer ->
        misc.forEachIndexed { i, r ->
            producer.send(ProducerRecord(topic, r.get("id").toString(), r))
            println("Sent message $i")
            Thread.sleep(1000L)
        }
        producer.flush()
        println("Successfully produced ${misc.size} messages to a topic called $topic")
    }
} catch (e: InterruptedException) {
    e.printStackTrace()
} catch (e: SerializationException) {
    e.printStackTrace()
}
```

#### Consumer for Kafka with AVRO format

```kotlin
properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = GlueSchemaRegistryKafkaDeserializer::class.java.name
properties[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
properties[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = AvroRecordType.GENERIC_RECORD.getName()

KafkaConsumer<String, GenericRecord>(properties).use { consumer ->
    consumer.subscribe(listOf(topic))

    while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        for (record in records) {
            val key = record.key()
            val value = record.value()
            println("Received message: key = $key, value = $value")
        }
    }
}
```

#### Producer for Kafka with JSON format

```kotlin
properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = GlueSchemaRegistryKafkaSerializer::class.java.name
properties[AWSSchemaRegistryConstants.DATA_FORMAT] = DataFormat.JSON.name
properties[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
properties[AWSSchemaRegistryConstants.REGISTRY_NAME] = "my-registry"
properties[AWSSchemaRegistryConstants.SCHEMA_NAME] = "my-schema"

val jsonSchema =
    """
    {
      "${'$'}schema": "http://json-schema.org/draft-04/schema#",
      "type": "object",
      "properties": {
        "employee": {
          "type": "object",
          "properties": {
            "name": { "type": "string" },
            "age": { "type": "integer" },
            "city": { "type": "string" }
          },
          "required": ["name", "age", "city"]
        }
      },
      "required": ["employee"]
    }
    """.trimIndent()

val jsonPayload =
    """
    {
      "employee": {
        "name": "John",
        "age": 30,
        "city": "New York"
      }
    }
    """.trimIndent()

val jsonSchemaWithData = JsonDataWithSchema.builder(jsonSchema, jsonPayload).build()

val genericJsonRecords = listOf(jsonSchemaWithData)

try {
    KafkaProducer<String, JsonDataWithSchema>(properties).use { producer ->
        genericJsonRecords.forEachIndexed { i, r ->
            producer.send(ProducerRecord(topic, "message-$i", r))
            println("Sent message $i")
            Thread.sleep(1000L)
        }
        producer.flush()
        println("Successfully produced ${genericJsonRecords.size} messages to a topic called $topic")
    }
} catch (e: InterruptedException) {
    e.printStackTrace()
} catch (e: SerializationException) {
    e.printStackTrace()
}
```

#### Consumer for Kafka with JSON format

```kotlin
properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = GlueSchemaRegistryKafkaDeserializer::class.java.name
properties[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"

KafkaConsumer<String, JsonDataWithSchema>(properties).use { consumer ->
    consumer.subscribe(listOf(topic))

    while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        for (record in records) {
            val key = record.key()
            val value = record.value()
            println("Received message: key = $key, value = $value")
        }
    }
}
```

#### Producer for Kafka with PROTOBUF format

```kotlin
properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = GlueSchemaRegistryKafkaSerializer::class.java.name
properties[AWSSchemaRegistryConstants.DATA_FORMAT] = DataFormat.PROTOBUF.name
properties[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
properties[AWSSchemaRegistryConstants.REGISTRY_NAME] = "my-registry"
properties[AWSSchemaRegistryConstants.SCHEMA_NAME] = "protobuf-file-name.proto"

// POJO production

// CustomerAddress is the generated Protocol Buffers class based on the given Protobuf schema definition
val customerAddress = CustomerAddress.newBuilder().build()

val pojoProducer = KafkaProducer<String, CustomerAddress>(properties)

pojoProducer.send(ProducerRecord(topic, customerAddress))

// DynamicMessage production

val customerDynamicMessage = DynamicMessage.newBuilder(CustomerAddress.getDescriptor()).build()

val dynamicMessageProducer = KafkaProducer<String, DynamicMessage>(properties)

dynamicMessageProducer.send(ProducerRecord(topic, customerDynamicMessage))
```

#### Consumer for Kafka with PROTOBUF format

```kotlin
properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = GlueSchemaRegistryKafkaDeserializer::class.java.name
properties[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"

// POJO consumption

properties[AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE] = ProtobufMessageType.POJO.getName()

val pojoConsumer = KafkaConsumer<String, CustomerAddress>(properties)

pojoConsumer.subscribe(listOf(topic))

val pojoRecords = pojoConsumer.poll(Duration.ofMillis(10))
pojoRecords.forEach { record -> processRecord(record) }

// DynamicMessage consumption

// This is optional. By default AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE is set as ProtobufMessageType.DYNAMIC_MESSAGE.getName()
properties[AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE] = ProtobufMessageType.DYNAMIC_MESSAGE.getName()

val dynamicMessageConsumer = KafkaConsumer<String, DynamicMessage>(properties)

dynamicMessageConsumer.subscribe(listOf(topic))

val dynamicMessageRecords = dynamicMessageConsumer.poll(Duration.ofMillis(10))
dynamicMessageRecords.forEach { record -> processRecord(record) }
```

### Dealing with Specific Record (JAVA POJO) for JSON

You could use a POJO and pass the object as a record.
We use [mbknor-jackson-jsonschema](https://github.com/mbknor/mbknor-jackson-jsonSchema) to generate a JSON Schema for
the POJO passed. This library can also inject additional information in the JSON Schema.

**GSR Library uses the "className" fully qualified class name to deserialize back to an Object of the POJO.
Introduced in 2.0.0; disabled by default — see
[Deserializing JSON into a Java POJO (className resolution)](#deserializing-json-into-a-java-pojo-classname-resolution).
Until you enable it, the deserializer returns a `JsonDataWithSchema` even when the schema carries a `className`.**

Example class :

```kotlin
// List of annotations to help infer JSON Schema are defined by https://github.com/mbknor/mbknor-jackson-jsonSchema
@JsonSchemaDescription("This is a car")
@JsonSchemaTitle("Simple Car Schema")
// Fully qualified class name to be added to an additionally injected property
// called className for deserializer to determine which class to deserialize
// the bytes into
@JsonSchemaInject(
    strings = [
        JsonSchemaString(
            path = "className",
            value = "com.amazonaws.services.schemaregistry.integrationtests.generators.Car",
        ),
    ],
)
// A default on every property makes Kotlin emit the no-arg constructor Jackson
// needs to deserialize bytes into an object of this class
class Car(
    @JsonProperty(required = true)
    val make: String? = null,
    @JsonProperty(required = true)
    val model: String? = null,
    @JsonSchemaDefault("true")
    @JsonProperty
    val used: Boolean = false,
    @JsonSchemaInject(ints = [JsonSchemaInt(path = "multipleOf", value = 1000)])
    @Max(200000)
    @JsonProperty
    val miles: Int = 0,
    @Min(2000)
    @JsonProperty
    val year: Int = 0,
    @JsonProperty
    val purchaseDate: Date? = null,
    @JsonProperty
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    val listedDate: Date? = null,
    @JsonProperty
    val owners: Array<String>? = null,
    @JsonProperty
    val serviceChecks: Collection<Float>? = null,
)
```

### Deserializing JSON into a Java POJO (className resolution)

By default the JSON deserializer returns a `JsonDataWithSchema`, even when the schema carries a
`className` property. Resolving that property would let the schema decide which class the
deserializer instantiates via reflection, so it must be opted into explicitly.

To deserialize into your POJO, set **both** properties on the consumer:

```kotlin
// Opt in to reading the schema's "className" property. Defaults to false.
properties[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = true

// Comma-separated list of fully qualified class names the deserializer may instantiate.
// Defaults to empty, so this must be set for the flag above to have any effect.
properties[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = "com.example.Car,com.example.Truck"
```

An entry ending in `.*` allows every class directly in that package, which avoids listing each POJO
individually:

```kotlin
properties[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = "com.example.pojos.*"
```

Notes:

- Setting `JSON_CLASS_NAME_RESOLUTION_ENABLED` on its own has no effect — with an empty allowlist
  every record still deserializes to `JsonDataWithSchema`.
- A record whose `className` matches no allowlist entry deserializes to `JsonDataWithSchema` and
  logs a WARN naming the class, once per distinct class name rather than once per record. Past 100
  distinct names the deserializer logs that it is suppressing further warnings and stops, so a
  stream of unrecognized class names can neither flood the log nor grow its dedup state without
  bound.
- List only the classes you actually expect on the topic. Each entry is one class the deserializer
  is permitted to construct from data it received.
- A package entry matches direct members only: `com.example.pojos.*` allows
  `com.example.pojos.Car` but not `com.example.pojos.nested.Car`. Entries are matched literally,
  not as regular expressions, and a bare `*` is rejected.
- Prefer naming classes explicitly. A package entry also allows any class added to that package
  later, which is a decision you make once here rather than reviewing when the class appears.

**This is a breaking behavior change in 2.0.0.** Consumers that previously relied on automatic POJO
deserialization must set both properties to keep working; otherwise they will receive
`JsonDataWithSchema` and fail on the cast.

### Using AWS Glue Schema Registry with Kinesis Data Streams

**Kinesis Client library (KCL) / Kinesis Producer Library (KPL):** [Getting started with AWS Glue Schema Registry with AWS Kinesis Data Streams](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-kds)

If you cannot use KCL / KPL libraries for Kinesis Data Streams integration,
see [examples](examples/) and [integration-tests](integration-tests/) for a working example with Kinesis SDK, KPL and
KCL.

### Using Auto-Registration

Auto-Registration allows any record produced with new schema to be automatically registered with the AWS Glue Schema
Registry. The Schema is registered automatically and a new schema version is created and evolution checks are performed.

If the Schema already exists, but the schema version is new, the new schema version is created and evolution checks are performed.

Auto-Registration is disabled by default. To enable Auto-Registration, enable setting by passing the configuration to
the Producer as below :

```kotlin
properties[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true // If not passed, defaults to false
```

### Providing Registry Name

Registry Name can be provided by setting this property -

```kotlin
properties[AWSSchemaRegistryConstants.REGISTRY_NAME] = "my-registry" // If not passed, uses "default-registry"
```

### Providing Schema Name

Schema Name can be provided by setting this property -

```kotlin
properties[AWSSchemaRegistryConstants.SCHEMA_NAME] = "my-schema" // If not passed, uses transport name (topic name in case of Kafka)
```

Alternatively, a schema registry naming strategy implementation can be provided.

```kotlin
properties[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS] =
    "com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy"
```

An example test implementation class is [here](https://github.com/mobsuccess-devops/aws-glue-schema-registry/blob/master/serializer-deserializer/src/test/kotlin/com/amazonaws/services/schemaregistry/serializers/avro/CustomerProvidedSchemaNamingStrategy.kt).

### Providing Registry Description

Registry Description can be provided by setting this property -

```kotlin
properties[AWSSchemaRegistryConstants.DESCRIPTION] = "This registry is used for several purposes." // If not passed, constructs a description
```

### Providing Compatibility Setting for Schema

Registry Description can be provided by setting this property -

```kotlin
properties[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING] = Compatibility.FULL // Pass a compatibility mode. If not passed, uses Compatibility.BACKWARD
```

### Using Compression

Deserialized byte array can be compressed to save on data usage over the network and storage on the topic/stream. The
Consumer side using AWS Glue Schema Registry Deserializer would be able to decompress and deserialize the byte array.
By default, compression is disabled. Customers can choose ZLIB as compressionType by setting up below property.

```kotlin
// If not passed, defaults to no compression
properties[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name
```

### In-Memory Cache settings

In Memory cache is used by Producer to store schema to schema version id mapping and by consumer to store schema
version id to schema mapping. This cache allows Producers and Consumers to save time and hits on IO calls to Schema
Registry.

The cache is available by default. However, it can be fine-tuned by providing cache specific properties.

```kotlin
properties[AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS] = "60000" // If not passed, defaults to 24 Hours
properties[AWSSchemaRegistryConstants.CACHE_SIZE] = "100" // Maximum number of elements in a cache - If not passed, defaults to 200
```

### Migrating from a third party Schema Registry

To migrate to AWS Glue Schema Registry from a third party schema registry for AVRO data types for Kafka, add this
property for value class along with the third party jar.

```kotlin
properties[AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER] = ThirdPartyKafkaDeserializer::class.java.name
```

### Using Kafka Connect with AWS Glue Schema Registry

- Clone this repo, build and copy dependencies

```bash
git clone git@github.com:mobsuccess-devops/aws-glue-schema-registry.git
cd aws-glue-schema-registry
./gradlew :schema-registry-kafkaconnect-converter:shadowJar
```

The resulting uber-jar under `avro-kafkaconnect-converter/build/libs/` already bundles every
dependency, so there is no separate dependency-copy step.

- Configure Kafka Connectors with following properties

When configuring Kafka Connect workers or connectors, use the value of the string constant properties in the [AWSSchemaRegistryConstants](https://github.com/mobsuccess-devops/aws-glue-schema-registry/blob/master/common/src/main/kotlin/com/amazonaws/services/schemaregistry/utils/AWSSchemaRegistryConstants.kt) class to configure the AWSKafkaAvroConverter.

```properties
key.converter=com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter
value.converter=com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter
key.converter.region=ca-central-1
value.converter.region=ca-central-1
key.converter.schemaAutoRegistrationEnabled=true
value.converter.schemaAutoRegistrationEnabled=true
key.converter.avroRecordType=GENERIC_RECORD
value.converter.avroRecordType=GENERIC_RECORD
key.converter.schemaName=KeySchema
value.converter.schemaName=ValueSchema
```

As Glue Schema Registry is a fully managed service by AWS, there is no notion of schema registry URLs. Name of the registry (within the same AWS account) can be optionally configured using following options. If not specified, default-registry is used.

```properties
key.converter.registry.name=my-registry
value.converter.registry.name=my-registry
```

- Make the converter visible to the workers

  The uber-jar is self-contained, so it only has to be on the worker's classpath. Either drop
  it into a directory listed in the worker's `plugin.path`:

  ```properties
  plugin.path=/opt/kafka/connect-plugins
  ```

  or, for a standalone worker started through `kafka-run-class.sh`, put it on `CLASSPATH`:

  ```bash
  export CLASSPATH="$CLASSPATH:/path/to/schema-registry-kafkaconnect-converter-<version>.jar"
  ```

  Do not add `schema-registry-common` or `schema-registry-serde` alongside it: the uber-jar
  already bundles them, and a second copy on the classpath is how duplicate-class failures
  start.

- (Optional) If you wish to test with a simple file source then clone the file source connector.

  ```bash
      git clone https://github.com/mmolimar/kafka-connect-fs.git
      cd kafka-connect-fs/
  ```

  Under source connector configuration(config/kafka-connect-fs.properties), edit the data format to Avro, file reader
  to AvroFileReader and update an
  example Avro object from the file path you are reading from. For example:

  ```
      fs.uris=<path to a sample avro object>
      policy.regexp=^.*\.avro$
      file_reader.class=com.github.mmolimar.kafka.connect.fs.file.reader.AvroFileReader
  ```

  Install the source connector. `kafka-connect-fs` is a third-party project and builds with
  its own Maven build:

  ```bash
  mvn clean package
  export CLASSPATH="$CLASSPATH:$(find target/ -type f -name '*.jar' | grep -- '-package' | tr '\n' ':')"
  ```

  The commands below assume `KAFKA_HOME` points at your Apache Kafka installation.

  Update the sink properties under _<your Apache Kafka installation directory>/config/connect-file-sink.properties_

  ```
  file=<output file full path>
  topics=<my topic>
  ```

  Start Source Connector (In this example it is file source connector)

  ```
  $KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties config/kafka-connect-fs.properties
  ```

  Run Sink Connector (In this example it is file sink connector))

  ```
  $KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties $KAFKA_HOME/config/connect-file-sink.properties
  ```

- For more examples for running Kafka Connect with Avro, JSON, and Protobuf formats, refer script **run-local-tests.sh** under
  **integration-tests** module.

### Using Kafka Streams with AWS Glue Schema Registry

### Gradle dependency

```kotlin
implementation("com.mobsuccess:schema-registry-kafkastreams-serde:<version>")
```

```kotlin
val props = Properties()
props[StreamsConfig.APPLICATION_ID_CONFIG] = "avro-streams"
props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
props[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = AWSKafkaAvroSerDe::class.java.name
props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

props[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
props[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true
props[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = AvroRecordType.GENERIC_RECORD.getName()

val builder = StreamsBuilder()
val source: KStream<String, GenericRecord> = builder.stream("avro-input")
val result =
    source
        .filter { _, value -> "pink" != value.get("favorite_color").toString() }
        .filter { _, value -> "15.0" != value.get("amount").toString() }
result.to("avro-output")

val streams = KafkaStreams(builder.build(), props)
streams.start()
```

## Using the AWS Glue Schema Registry Flink Connector

AWS Glue Schema Registry Flink Connector for Java in this repository is not recommended. Please check out [Apache Flink](https://github.com/apache/flink)
repository for the latest support: [Avro SerializationSchema and DeserializationSchema](https://github.com/apache/flink/tree/master/flink-formats/flink-avro-glue-schema-registry) and [JSON SerializationSchema and DeserializationSchema](https://github.com/apache/flink/tree/master/flink-formats/flink-json-glue-schema-registry). Protobuf integration will be followed up soon.

### Gradle dependency

```kotlin
implementation("com.mobsuccess:schema-registry-flink-serde:<version>")
```

### Code Example

#### Flink Kafka Producer with AVRO format

```kotlin
val topic = "topic"
val properties = Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")

val configs =
    mapOf<String, Any>(
        AWSSchemaRegistryConstants.AWS_REGION to "us-east-1",
        AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING to true,
    )

val schema = Schema.Parser().parse(File("path/to/avro/file"))

val producer =
    FlinkKafkaProducer(
        topic,
        GlueSchemaRegistryAvroSerializationSchema.forGeneric(schema, topic, configs),
        properties,
    )
stream.addSink(producer)
```

#### Flink Kafka Consumer with AVRO format

```kotlin
val topic = "topic"
val properties = Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")

val configs =
    mapOf<String, Any>(
        AWSSchemaRegistryConstants.AWS_REGION to "us-east-1",
        AWSSchemaRegistryConstants.AVRO_RECORD_TYPE to AvroRecordType.GENERIC_RECORD.getName(),
    )

val schema = Schema.Parser().parse(File("path/to/avro/file"))

val consumer =
    FlinkKafkaConsumer(
        topic,
        GlueSchemaRegistryAvroDeserializationSchema.forGeneric(schema, configs),
        properties,
    )
val stream: DataStream<GenericRecord> = env.addSource(consumer)
```

## Cross-Account Avro Converter Support

The `AWSKafkaAvroConverter` Avro converter is able to assume an IAM role in a different AWS account before accessing Glue Schema Registry. You can configure the role ARN and an optional session name.

If `assumeRoleArn` is not provided, the converter will fallback to using the default credentials associated to the host.

### Connector configuration

Include these properties in your Kafka Connect worker or connector config:

```properties
# Define converter
key.converter=com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter
value.converter=com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter

# Specify cross-account role arn
key.converter.assumeRoleArn="arn:aws:iam::123456789012:role/my-role"
value.converter.assumeRoleArn="arn:aws:iam::123456789012:role/my-role"

# Override default session name (optional; default is "kafka-connect-session")
key.converter.assumeRoleSessionName=my-custom-session
value.converter.assumeRoleSessionName=my-custom-session
```

## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.

## License and attribution

This library is distributed under the [Apache License 2.0](LICENSE.txt), unchanged from
upstream. This fork is a **modified** distribution of
[`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry):
the modifications are listed in [NOTICE.txt](NOTICE.txt) and summarised in
[How this fork differs](#how-this-fork-differs). It is neither endorsed by nor affiliated
with Amazon.com, Inc. or its affiliates.

Every published artifact carries `META-INF/LICENSE.txt` and `META-INF/NOTICE.txt`. The
uber-jars — the Kafka Connect converters and `schema-registry-serde-msk-iam` — bundle
their dependencies, and so also carry `META-INF/THIRD-PARTY-LICENSES.txt`, an inventory
generated from the resolved runtime classpath at build time:

```bash
./gradlew thirdPartyLicenses
```
