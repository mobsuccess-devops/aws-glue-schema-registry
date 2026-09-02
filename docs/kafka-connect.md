# Kafka Connect converters

Three converters are published, one per data format:

| Artifact                                 | Converter class                                                                       | Format      |
| ---------------------------------------- | ------------------------------------------------------------------------------------- | ----------- |
| `schema-registry-kafkaconnect-converter` | `com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter`            | AVRO        |
| `jsonschema-kafkaconnect-converter`      | `com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaConverter`   | JSON Schema |
| `protobuf-kafkaconnect-converter`        | `com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ProtobufSchemaConverter` | Protobuf    |

Each is an ordinary jar with a complete pom, and each also ships a **plugin distribution**:
a zip holding the jar and every dependency it resolves, laid out as a Connect plugin
directory. The keys the converters accept are in the
[configuration reference](configuration.md), which also covers what `Converter.config()`
validates and what it lets through.

## Get the plugin distribution

Every [release](https://github.com/mobsuccess-devops/aws-glue-schema-registry/releases)
carries one zip per converter, next to the `SHA256SUMS.txt` that covers it:

```bash
gh release download v<version> \
  --repo mobsuccess-devops/aws-glue-schema-registry \
  --pattern 'schema-registry-kafkaconnect-converter-*-plugin.zip'
```

Or build it from source:

```bash
git clone git@github.com:mobsuccess-devops/aws-glue-schema-registry.git
cd aws-glue-schema-registry
./gradlew :schema-registry-kafkaconnect-converter:pluginDistribution
```

The zip lands in `avro-kafkaconnect-converter/build/distributions/`. It contains a single
directory, `<artifactId>-<version>/`, with every jar under `lib/` and the licences beside
them — the layout a Connect worker expects, and the one Confluent Hub packages use.

## Configure the connector

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

`schemaName` pins one name per converter, which is one way to keep a topic's key schema and value
schema from colliding on a single registry entry. The other is to let the naming strategy do it,
which keeps the topic in the name:

```properties
key.converter.schemaNameGenerationClass=com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategyTopicNameImpl
value.converter.schemaNameGenerationClass=com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategyTopicNameImpl
```

A converter reading `orders` then registers `orders-key` and `orders-value`, the Confluent
`TopicNameStrategy` names. Details and the migration caveat are in
[configuration.md](configuration.md#naming-a-key-apart-from-a-value).

As Glue Schema Registry is a fully managed service by AWS, there is no notion of schema registry URLs. Name of the registry (within the same AWS account) can be optionally configured using following options. If not specified, default-registry is used.

```properties
key.converter.registry.name=my-registry
value.converter.registry.name=my-registry
```

## Make the converter visible to the workers

Unzip the distribution into a directory listed in the worker's `plugin.path`:

```bash
unzip schema-registry-kafkaconnect-converter-<version>-plugin.zip -d /opt/kafka/connect-plugins
```

```properties
plugin.path=/opt/kafka/connect-plugins
```

That leaves `/opt/kafka/connect-plugins/schema-registry-kafkaconnect-converter-<version>/lib/`
holding the converter and its dependencies, which is one plugin location: the worker builds
an isolated classloader over it and finds the converter class there. Do not unzip two
converters into the same directory — one plugin location means one classloader, and the point
of the layout is that each converter gets its own.

For a standalone worker started through `kafka-run-class.sh`, put the same `lib/` on
`CLASSPATH` instead:

```bash
export CLASSPATH="$CLASSPATH:/opt/kafka/connect-plugins/schema-registry-kafkaconnect-converter-<version>/lib/*"
```

The distribution includes `slf4j-api`. Connect supplies its own, and the plugin classloader
delegates `org.slf4j` to the parent, so the converter logs through the worker's stack and the
bundled copy is never loaded — nothing to remove.

**If you resolve the converter through Maven or Gradle instead**, the pom declares everything
and the build tool does the rest; there is no plugin zip involved and nothing to exclude:

```kotlin
implementation("com.mobsuccess:schema-registry-kafkaconnect-converter:<version>")
```

## Optional: trying it with a file source connector

```bash
    git clone https://github.com/mmolimar/kafka-connect-fs.git
    cd kafka-connect-fs/
```

In the source connector configuration, `config/kafka-connect-fs.properties`, set the data
format to Avro and the file reader to `AvroFileReader`, and point it at an example Avro object
from the path you are reading from:

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

Start the source connector (here, the file source connector):

```
$KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties config/kafka-connect-fs.properties
```

Run the sink connector (here, the file sink connector):

```
$KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties $KAFKA_HOME/config/connect-file-sink.properties
```

For more examples of running Kafka Connect with Avro, JSON and Protobuf, see
`run-local-tests.sh` in the [integration-tests](../integration-tests/) module.

## Cross-account access

The `AWSKafkaAvroConverter` Avro converter is able to assume an IAM role in a different AWS account before accessing Glue Schema Registry. You can configure the role ARN and an optional session name.

If `assumeRoleArn` is not provided, the converter falls back to the default credentials associated with the host.

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
