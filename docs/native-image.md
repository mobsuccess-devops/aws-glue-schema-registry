# GraalVM native image

This library runs inside a GraalVM native image, and `schema-registry-serde` and
`schema-registry-kafkastreams-serde` ship the reachability metadata that makes it work. A
consumer building a native image writes **no native configuration for this library** — only
for its own classes.

Nothing here is related to `native-schema-registry`, the C shared library upstream compiled
ahead of time to give its C# binding an entry point. That module was removed with the binding;
see [portage.md](portage.md).

## What the jars declare

`native-image` reads `META-INF/native-image/<group>/<artifact>/` off the classpath
automatically, so adding the dependency is the whole setup.

| Artifact                             | File                   | Contents                                                                                                                                                                                                  |
| ------------------------------------ | ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `schema-registry-serde`              | `reflect-config.json`  | The four entry points a Kafka configuration names as a string — `GlueSchemaRegistryKafka{Serializer,Deserializer}`, `AWSKafkaAvro{Serializer,Deserializer}` — plus `com.google.protobuf.DescriptorProtos` |
| `schema-registry-serde`              | `resource-config.json` | The 29 `.proto` files `ProtobufSchemaLoader` reads with `getResourceAsStream`                                                                                                                             |
| `schema-registry-kafkastreams-serde` | `reflect-config.json`  | `GlueSchemaRegistryKafkaStreamsSerde` and `AWSKafkaAvroSerDe`                                                                                                                                             |

Both categories exist because nothing references them statically. A serializer named in a
producer property is reached by `Class.forName`, and a `.proto` read off the classpath is not
embedded in an image unless it is declared — its absence does not fail the build, it throws
`IOException: Proto file not found` at run time, on the Protobuf path only.

## What you still have to declare

The metadata covers this library, not your application:

- **Your own record types** — generated Avro `SpecificRecord` classes, generated protobuf
  classes, and the POJOs behind `AVRO_SPECIFIC_RECORD` or the JSON POJO path. They are
  instantiated and populated reflectively, and only you know their names.
- **The strategies and classes you name by string** in a configuration — a custom
  `AWSSchemaNamingStrategy`, a `secondaryDeserializer`, a custom `Serde`.
- **Anything your framework already asks for.** On Quarkus that is `@RegisterForReflection`
  or `quarkus.native.additional-build-args`; on Spring Native, the equivalent hints.

## Verification, and its limits

The metadata was verified in
[PR #108](https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/108) against a
real consumer: a Quarkus application that deploys as a native image. Its native build
previously needed a Substrate substitution amputating the JSON and Protobuf formats; with
these jars it builds with no Glue Schema Registry native configuration of its own. That
the metadata is actually consumed was checked by diffing the build output rather than by the
build passing — an unresolvable entry is ignored in silence — and the 29 protos show up as
exactly 29 extra resources.

**That verification was done before protobuf 4.** [PR #135](https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/135)
moved the build from protobuf 3.25.5 to 4.36.0, which changed the runtime's own reflection
internals, and the native smoke test has **not** been re-run since. Consider native-image
support verified for the 2.x releases and unverified on `master`. If you build a native image
off the next major, expect to check it yourself, and please report what you find.

The native binary has never been _run_ against a real Kafka broker and Glue registry in this
repository; the check is a successful build with the metadata provably consumed.

## The drift guard

A metadata file that nothing checks goes stale, so three test classes hold it in place:

| Test                                            | What it asserts                                                                                                                                                                          |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ReflectConfigTest` (`serializer-deserializer`) | The declared class set is exactly the expected one, every declared constructor resolves, and every entry point is still a Kafka `Serializer` or `Deserializer` with a no-arg constructor |
| `ReflectConfigTest` (`kafkastreams-serde`)      | The same, for the two `Serde` classes                                                                                                                                                    |
| `ProtobufSchemaLoaderResourceConfigTest`        | The declared resource list equals the loader's own `GOOGLE_API_PROTOS` / `GOOGLE_WELLKNOWN_PROTOS` / `WIRE_PROTOS` sets, and every declared resource exists on the classpath             |

The consequence for contributors: **the proto list moves with the loader**. Adding a proto to
one of those sets without adding it to `resource-config.json` compiles, passes every test on
the JVM, and breaks only inside a native image — which is exactly what the third test refuses
to let happen. See [build.md](build.md).
