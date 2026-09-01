# Flink connector

`schema-registry-flink-serde` provides Avro `SerializationSchema` and
`DeserializationSchema` implementations backed by the AWS Glue Schema Registry, for use
anywhere Flink takes one — a Kafka source or sink, a Kinesis connector, a file format.

**Flink 1.20.x (LTS).** The catalog pins `org.apache.flink:flink-avro` and
`org.apache.flink:flink-streaming-java` at 1.20.5, and the module is built and tested
against that line. Flink is not bundled: `flink-avro` is an `api` dependency and
`flink-streaming-java` is `compileOnly`, so the version a job runs is the cluster's own.

**A jar built here needs a Flink runtime of 1.19 or later.** `AvroSerializationSchema.getEncoder()`
returns `Encoder` from 1.19 on, where 1.18 and earlier returned `BinaryEncoder`;
`GlueSchemaRegistryAvroSerializationSchema.serialize` calls it, so the compiled call site
resolves to a method signature that does not exist on an older Flink and fails with
`NoSuchMethodError`. 1.20.x is what the module is tested against.

Alternatively, Apache Flink ships its own Glue Schema Registry formats —
[Avro](https://github.com/apache/flink/tree/master/flink-formats/flink-avro-glue-schema-registry)
and
[JSON](https://github.com/apache/flink/tree/master/flink-formats/flink-json-glue-schema-registry).
They cover the same ground for Avro and JSON; this module is the one to use for a job
already on the rest of this fork, or one that needs the serializer configuration surface
documented in [configuration.md](configuration.md).

## Gradle dependency

```kotlin
implementation("com.mobsuccess:schema-registry-flink-serde:<version>")
```

The Kafka connector is separate, and versioned against a Flink line rather than with it:

```kotlin
implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")
```

## Code examples

### Kafka sink with AVRO format

```kotlin
val topic = "topic"

val configs =
    mapOf<String, Any>(
        AWSSchemaRegistryConstants.AWS_REGION to "us-east-1",
        AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING to true,
    )

val schema = Schema.Parser().parse(File("path/to/avro/file"))

val sink =
    KafkaSink
        .builder<GenericRecord>()
        .setBootstrapServers("localhost:9092")
        .setRecordSerializer(
            KafkaRecordSerializationSchema
                .builder<GenericRecord>()
                .setTopic(topic)
                .setValueSerializationSchema(
                    GlueSchemaRegistryAvroSerializationSchema.forGeneric(schema, topic, configs),
                ).build(),
        ).build()

stream.sinkTo(sink)
```

### Kafka source with AVRO format

```kotlin
val topic = "topic"

val configs =
    mapOf<String, Any>(
        AWSSchemaRegistryConstants.AWS_REGION to "us-east-1",
        AWSSchemaRegistryConstants.AVRO_RECORD_TYPE to AvroRecordType.GENERIC_RECORD.getName(),
    )

val schema = Schema.Parser().parse(File("path/to/avro/file"))

val source =
    KafkaSource
        .builder<GenericRecord>()
        .setBootstrapServers("localhost:9092")
        .setGroupId("test")
        .setTopics(topic)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(
            GlueSchemaRegistryAvroDeserializationSchema.forGeneric(schema, configs),
        ).build()

val stream: DataStream<GenericRecord> =
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "glue-schema-registry-avro")
```

`FlinkKafkaProducer` and `FlinkKafkaConsumer`, which the upstream examples used, are
deprecated in `flink-connector-kafka` 3.x and gone in 4.x. They still resolve on Flink
1.20 and the schemas above work with them unchanged, but new jobs should take
`KafkaSink` and `KafkaSource`.
