# Flink connector

> **This connector is not recommended for new work.** It is carried over from upstream
> unchanged and pinned to Flink 1.12.2 with `flink-streaming-java_2.11`, a Scala 2.11
> coordinate Flink stopped publishing after 1.14. It is kept so the fork stays
> behaviour-identical to its source, not because it is a reasonable dependency to take today.
> Use the Glue Schema Registry formats that ship with Apache Flink itself instead:
> [Avro](https://github.com/apache/flink/tree/master/flink-formats/flink-avro-glue-schema-registry)
> and [JSON](https://github.com/apache/flink/tree/master/flink-formats/flink-json-glue-schema-registry).

## Gradle dependency

```kotlin
implementation("com.mobsuccess:schema-registry-flink-serde:<version>")
```

## Code examples

### Flink Kafka Producer with AVRO format

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

### Flink Kafka Consumer with AVRO format

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
