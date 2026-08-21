# AWS Glue Schema Registry Sample Code

Code samples for integrating with Glue Schema Registry. The module is built with the rest
of the repository — there is no Maven build here.

## Kinesis Data Streams PutRecords / GetRecords

Glue Schema Registry can be used with the PutRecords / GetRecords APIs of Kinesis Data
Streams to encode and decode data. See
[`PutRecordGetRecordExample`](src/main/kotlin/com/amazonaws/services/schemaregistry/examples/kds/PutRecordGetRecordExample.kt)
for how to integrate.

### Running the sample

Create a stream on Kinesis Data Streams.

```bash
aws kinesis create-stream --stream-name testStream --shard-count 1 --region us-west-2
```

Build the module.

```bash
./gradlew :schema-registry-examples:assemble
```

Run the sample. `runExample` is a `JavaExec` task declared in
[`build.gradle.kts`](build.gradle.kts); it runs from this module's directory, which is what the
sample's relative path to `src/main/resources/user.avsc` needs. Everything after `--args` is
passed to the program.

```bash
./gradlew :schema-registry-examples:runExample --args="--region us-west-2 --stream testStream --numRecords 5 --schema testGsrSchema"
```

The sample resolves credentials through the default AWS provider chain, so the usual
`AWS_PROFILE` and `AWS_REGION` environment variables and any active SSO session apply.
