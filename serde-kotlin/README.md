# schema-registry-serde-kotlin

Kotlin conveniences over `schema-registry-serde`. Additive only: it introduces no behaviour of
its own, builds the same objects the Java API builds, and nothing in the other modules depends
on it.

Two things it gives you, both of which the Java API makes awkward:

- a **configuration DSL** with one typed property per key, instead of a `Map<String, Object>` of
  string literals;
- a **`Serde<T>`**, instead of the `Serde<Any>` Kafka Streams gets today.

```gradle
implementation("com.mobsuccess:schema-registry-serde-kotlin:<version>")
```

## Configuration

```kotlin
import com.mobsuccess.schemaregistry.kotlin.glueSchemaRegistryConfig
import software.amazon.awssdk.services.glue.model.DataFormat

val properties = glueSchemaRegistryConfig {
    region = "eu-west-1"
    dataFormat = DataFormat.AVRO
    autoRegistration = true
    registryName = "my-registry"
    compression = AWSSchemaRegistryConstants.COMPRESSION.ZLIB
    tags(mapOf("owner" to "data-platform"))
}
```

`glueSchemaRegistryConfig` returns the plain `Map<String, Any>` every serializer, deserializer
and Connect converter already reads, so it drops into whatever you are configuring:

```kotlin
producerProperties.putAll(properties)
```

**Only the properties you set appear in the map.** A key you leave alone is absent, so the
library applies its own default rather than one the DSL invented — configuring through the DSL
and configuring through a map by hand produce the same `GlueSchemaRegistryConfiguration`.

Two keys the DSL gets right for you: `tags` and `metadata` are copied into a `HashMap`, which is
the one shape `GlueSchemaRegistryConfiguration` accepts for them — `mapOf("a" to "b")` returns a
`SingletonMap` and is rejected at runtime.

For a key with no typed property, and for anything added after this module was written:

```kotlin
property(AWSSchemaRegistryConstants.ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/my-role")
```

## Serializers and deserializers

```kotlin
val serializer = glueSchemaRegistrySerializer {
    region = "eu-west-1"
    dataFormat = DataFormat.AVRO
    autoRegistration = true
}

val deserializer = glueSchemaRegistryDeserializer {
    region = "eu-west-1"
    avroRecordType = AvroRecordType.SPECIFIC_RECORD
}
```

Both are the ordinary `GlueSchemaRegistryKafkaSerializer` and
`GlueSchemaRegistryKafkaDeserializer`, already `configure`d. Pass `isKey = true` for a key serde.

## A typed `Serde<T>` for Kafka Streams

`GlueSchemaRegistryKafkaStreamsSerde` is a `Serde<Any>`, so a `KStream` built on it carries
`Any` and every operator needs a cast:

```kotlin
val serde = glueSchemaRegistrySerde<User> {
    region = "eu-west-1"
    dataFormat = DataFormat.AVRO
    avroRecordType = AvroRecordType.SPECIFIC_RECORD
}

builder.stream("users", Consumed.with(Serdes.String(), serde))
    .filter { _, user -> user.age > 18 }   // user is a User
```

The registry decides at runtime what a record deserializes to, so `T` is a claim about the topic
rather than something the compiler can check. It is checked on **every record** instead: a value
of another type raises a `SerializationException` naming both types, at the point the record is
read, rather than a `ClassCastException` somewhere further down the topology.

An existing `Serde<Any>` can be viewed the same way:

```kotlin
val typed: Serde<User> = GlueSchemaRegistryKafkaStreamsSerde().typed()
```

## Java callers

The DSL entry points take a Kotlin lambda with receiver and are meant for Kotlin. `TypedSerde`
itself is an ordinary class and is usable from Java:

```java
Serde<User> serde = new TypedSerde<>(User.class, new GlueSchemaRegistryKafkaStreamsSerde());
```
