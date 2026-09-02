# Using the serializers and deserializers

`schema-registry-serde` carries the Kafka serializer and deserializer for the three supported
formats. Add it to the build — see [Installation](installation.md) for the repository setup:

```kotlin
implementation("com.mobsuccess:schema-registry-serde:<version>")
```

**With Amazon MSK** — to set up Amazon Managed Streaming for Apache Kafka, see
[Getting started with Amazon MSK](https://docs.aws.amazon.com/msk/latest/developerguide/getting-started.html).

Every property used below is listed, with its default and its scope, in the
[configuration reference](configuration.md).

## Code examples

### Producer for Kafka with AVRO format

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

### Consumer for Kafka with AVRO format

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

### Producer for Kafka with JSON format

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

### Consumer for Kafka with JSON format

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

### Producer for Kafka with PROTOBUF format

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

### Consumer for Kafka with PROTOBUF format

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

## Dealing with Specific Record (JAVA POJO) for JSON

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

## Nullable fields in a generated JSON Schema

When a schema is derived from a POJO, an optional field is typed as its type alone by default.
A POJO whose optional field is null then serializes to `"field": null` and fails validation
against its own generated schema, so the object cannot be sent at all.

Set this property on the producer to generate `oneOf [null, type]` for those fields instead:

```kotlin
// Defaults to false.
properties[AWSSchemaRegistryConstants.JSON_SCHEMA_NULLABLE_ENABLED] = true
```

It is off by default because it changes the schema text, and therefore registers a new schema
version for a POJO that was already in the registry.

## Checking JSON Schema compatibility on the client

Glue enforces the compatibility mode of a schema for Avro and Protobuf, but **not for JSON**: a
JSON schema version that breaks its declared mode is accepted by `RegisterSchemaVersion`, and the
breakage surfaces in a consumer instead of in the producer that caused it.

Set this property on the producer to have the library check before it registers:

```kotlin
// Defaults to false.
properties[AWSSchemaRegistryConstants.JSON_SCHEMA_COMPATIBILITY_CHECK_ENABLED] = true
```

When it is on, registering a new JSON schema version first reads the latest version of that
schema and compares the two against the configured `compatibility`. An incompatibility raises an
`AWSSchemaRegistryException` naming the field, and nothing is registered. A schema with no
previous version is registered without a check.

What is compared is the **`required` contract**, at the top level and inside each named entry of
`definitions` or `$defs`: under `BACKWARD` a field may not become required, under `FORWARD` a
required field may not stop being required, and `FULL` applies both. Types, formats, enumerations
and `additionalProperties` are _not_ compared — a clean result means "no broken `required`
contract", not "compatible". It is off by default for that reason, and because it costs one extra
`GetSchemaVersion` call per newly registered schema definition.

## Deserializing JSON into a Java POJO (className resolution)

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

## Using AWS Glue Schema Registry with Kinesis Data Streams

**Kinesis Client library (KCL) / Kinesis Producer Library (KPL):** [Getting started with AWS Glue Schema Registry with AWS Kinesis Data Streams](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-kds)

If you cannot use KCL / KPL libraries for Kinesis Data Streams integration,
see [examples](../examples/) and [integration-tests](../integration-tests/) for a working example with Kinesis SDK, KPL and
KCL.

## Using Auto-Registration

Auto-Registration allows any record produced with new schema to be automatically registered with the AWS Glue Schema
Registry. The Schema is registered automatically and a new schema version is created and evolution checks are performed.

If the Schema already exists, but the schema version is new, the new schema version is created and evolution checks are performed.

Auto-Registration is disabled by default. To enable Auto-Registration, enable setting by passing the configuration to
the Producer as below :

```kotlin
properties[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true // If not passed, defaults to false
```

## Providing Registry Name

Registry Name can be provided by setting this property -

```kotlin
properties[AWSSchemaRegistryConstants.REGISTRY_NAME] = "my-registry" // If not passed, uses "default-registry"
```

## Providing Schema Name

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

### Naming a topic's key and value apart

The default strategy names a schema after the transport alone, so the key and the value of one
topic register under the same schema name and overwrite each other's versions.
`AWSSchemaNamingStrategyTopicNameImpl` ships with the library and gives them the Confluent
`TopicNameStrategy` names — `<topic>-key` and `<topic>-value`:

```kotlin
properties[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS] =
    "com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategyTopicNameImpl"
```

The same property is set on both serializers; each one already knows which side it serializes,
from the `isKey` argument Kafka passes to `configure`. **The default is unchanged**: without this
property a schema is still named after the topic alone, which is what an existing registry
contains.

## Providing Registry Description

Registry Description can be provided by setting this property -

```kotlin
properties[AWSSchemaRegistryConstants.DESCRIPTION] = "This registry is used for several purposes." // If not passed, constructs a description
```

## Providing Compatibility Setting for Schema

The compatibility mode used when auto-registration creates the schema can be provided by
setting this property -

```kotlin
properties[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING] = Compatibility.FULL // Pass a compatibility mode. If not passed, uses Compatibility.BACKWARD
```

## Using Compression

Deserialized byte array can be compressed to save on data usage over the network and storage on the topic/stream. The
Consumer side using AWS Glue Schema Registry Deserializer would be able to decompress and deserialize the byte array.
By default, compression is disabled. Customers can choose ZLIB as compressionType by setting up below property.

```kotlin
// If not passed, defaults to no compression
properties[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name
```

## In-Memory Cache settings

In Memory cache is used by Producer to store schema to schema version id mapping and by consumer to store schema
version id to schema mapping. This cache allows Producers and Consumers to save time and hits on IO calls to Schema
Registry.

The cache is available by default. However, it can be fine-tuned by providing cache specific properties.

```kotlin
properties[AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS] = "60000" // If not passed, defaults to 24 Hours
properties[AWSSchemaRegistryConstants.CACHE_SIZE] = "100" // Maximum number of elements in a cache - If not passed, defaults to 200
```

## Migrating from a third party Schema Registry

To migrate to AWS Glue Schema Registry from a third party schema registry for AVRO data types for Kafka, add this
property for value class along with the third party jar.

```kotlin
properties[AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER] = ThirdPartyKafkaDeserializer::class.java.name
```

## Kotlin: configuration DSL and a typed `Serde<T>`

`schema-registry-serde-kotlin` is additive: it introduces no behaviour of its own, builds the
same objects the Java API builds, and nothing else depends on it. See
[its README](../serde-kotlin/README.md) for the full surface.

```gradle
implementation("com.mobsuccess:schema-registry-serde-kotlin:<version>")
```

One typed property per configuration key, instead of a map of string literals:

```kotlin
val properties = glueSchemaRegistryConfig {
    region = "eu-west-1"
    dataFormat = DataFormat.AVRO
    autoRegistration = true
    tags(mapOf("owner" to "data-platform"))
}
```

Only the properties you set appear in the result, so the library's own defaults still apply, and
`tags`/`metadata` are copied into the `HashMap` those keys require — `mapOf("a" to "b")` returns
a `SingletonMap` and is rejected at runtime.

A `Serde<T>`, where Kafka Streams gets a `Serde<Any>` today:

```kotlin
val serde = glueSchemaRegistrySerde<User> {
    region = "eu-west-1"
    dataFormat = DataFormat.AVRO
    avroRecordType = AvroRecordType.SPECIFIC_RECORD
}

builder.stream("users", Consumed.with(Serdes.String(), serde))
    .filter { _, user -> user.age > 18 }
```

The registry decides at runtime what a record deserializes to, so the type argument is checked on
every record: a value of another type raises a `SerializationException` naming both types, where
an unchecked cast would have failed further down the topology.

## Kafka Streams

Its own artifact carries the Streams serde:

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
