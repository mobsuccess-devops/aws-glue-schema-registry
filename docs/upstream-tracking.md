# Upstream tracking

Every open issue and pull request on
[`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry),
and how this fork relates to each. Upstream has been dormant since `eed1506` (2026-08-13) —
the fork point, and still its `master` HEAD — so its open queue is the closest thing the two
repositories have to a shared backlog. This file records what remains to adopt, what the fork
has shipped, and what it deliberately leaves aside — one section per status, most actionable
first.

- **[To address](#to-address)** — worth adopting: _planned_ when scheduled, _evaluate_ when
  the value or the fix still needs to be established.
- **[Addressed](#addressed)** — shipped in this fork; the note links the pull request or
  names the coverage.
- **[Out of scope](#out-of-scope)** — deliberately not pursued, with the reason. "C#", "Go"
  and "native" refer to the multi-language layer this fork removed.

Last sync: 2026-08-21, upstream at `eed1506`. Move the relevant row when a fork pull request
lands on one of these items; re-sync the whole inventory when upstream moves.

## To address

14 pull requests, 30 issues.

| Upstream                                                                 | Type  | Title                                                | Notes                                                                                                                                              |
| ------------------------------------------------------------------------ | ----- | ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| [#32](https://github.com/awslabs/aws-glue-schema-registry/issues/32)     | issue | Support for schema references                        | Evaluate, large — with [#198].                                                                                                                       |
| [#38](https://github.com/awslabs/aws-glue-schema-registry/issues/38)     | issue | Allow setting a reader schema in GenericDatumReader  | Evaluate.                                                                                                                                            |
| [#46](https://github.com/awslabs/aws-glue-schema-registry/issues/46)     | issue | Environment variables in Kafka Connect config        | Evaluate — Connect config providers may already cover it; document if so.                                                                            |
| [#93](https://github.com/awslabs/aws-glue-schema-registry/issues/93)     | issue | Key and value serializers use the same schema name   | Evaluate — naming-collision family, with PRs [#126], [#472] and issue [#471].                                                                        |
| [#101](https://github.com/awslabs/aws-glue-schema-registry/issues/101)   | issue | Better null checks and error messages                | Evaluate — partially covered by [mobsuccess#81] and [mobsuccess#94].                                                                                 |
| [#120](https://github.com/awslabs/aws-glue-schema-registry/issues/120)   | issue | HTTP client conflicts                                | Evaluate — with PRs [#240], [#263], [#303].                                                                                                          |
| [#126](https://github.com/awslabs/aws-glue-schema-registry/pull/126)     | PR    | Schema naming strategy for Kafka Connect             | Evaluate — naming-collision family, with issues [#93], [#471] and PR [#472].                                                                         |
| [#172](https://github.com/awslabs/aws-glue-schema-registry/issues/172)   | issue | Full compatibility mode not working for JSON         | Verify [mobsuccess#119] (client-side JSON compatibility check) covers the reported scenario.                                                         |
| [#198](https://github.com/awslabs/aws-glue-schema-registry/issues/198)   | issue | Schema references for Avro                           | Evaluate, large — with [#32].                                                                                                                        |
| [#199](https://github.com/awslabs/aws-glue-schema-registry/issues/199)   | issue | Naming strategies out of the box                     | Evaluate — naming family.                                                                                                                            |
| [#201](https://github.com/awslabs/aws-glue-schema-registry/issues/201)   | issue | No PROTOBUF SchemaDereferencingStrategy              | Evaluate.                                                                                                                                            |
| [#202](https://github.com/awslabs/aws-glue-schema-registry/issues/202)   | issue | Java 8 time support in JsonSerializer                | Evaluate — with PR [#320].                                                                                                                           |
| [#217](https://github.com/awslabs/aws-glue-schema-registry/pull/217)     | PR    | Fix nullable enum fields                             | Evaluate — verify whether [mobsuccess#116] already covers nullable enums.                                                                            |
| [#231](https://github.com/awslabs/aws-glue-schema-registry/issues/231)   | issue | Single jar for all converters                        | Evaluate packaging ask.                                                                                                                              |
| [#236](https://github.com/awslabs/aws-glue-schema-registry/issues/236)   | issue | Validate a schema without producing                  | Evaluate — the client-side checker of [mobsuccess#119] could be exposed as an offline API.                                                           |
| [#237](https://github.com/awslabs/aws-glue-schema-registry/pull/237)     | PR    | Logical types conversion for serializer/deserializer | Evaluate.                                                                                                                                            |
| [#240](https://github.com/awslabs/aws-glue-schema-registry/pull/240)     | PR    | Remove url http client                               | Evaluate — HTTP-client family, with [#263], [#303] and issue [#120].                                                                                 |
| [#250](https://github.com/awslabs/aws-glue-schema-registry/issues/250)   | issue | JSON Connect UnsupportedOperationException           | Evaluate with the JSON Schema converter family ([#368], [#369]).                                                                                     |
| [#263](https://github.com/awslabs/aws-glue-schema-registry/pull/263)     | PR    | Remove url-connector dep, accept a GlueClientBuilder | Evaluate — same family; also relates to issues [#341] and [#293].                                                                                    |
| [#266](https://github.com/awslabs/aws-glue-schema-registry/issues/266)   | issue | Secondary deserializer in Kafka Connect              | Evaluate.                                                                                                                                            |
| [#289](https://github.com/awslabs/aws-glue-schema-registry/issues/289)   | issue | ConnectSchemaToProtobufSchemaConverter NPE           | Evaluate — reproduce first; possibly adjacent to [mobsuccess#67].                                                                                    |
| [#290](https://github.com/awslabs/aws-glue-schema-registry/issues/290)   | issue | Protobuf converter does not sanitize names           | Evaluate.                                                                                                                                            |
| [#291](https://github.com/awslabs/aws-glue-schema-registry/issues/291)   | issue | Upgrade the Flink version                            | Pending the Flink decision — with PR [#374] and issue [#371].                                                                                        |
| [#293](https://github.com/awslabs/aws-glue-schema-registry/issues/293)   | issue | Support a user-provided credentials provider         | Evaluate — `assumeRole` exists; a pluggable provider is the general form.                                                                            |
| [#296](https://github.com/awslabs/aws-glue-schema-registry/issues/296)   | issue | Schema information from message headers              | Evaluate, large.                                                                                                                                     |
| [#303](https://github.com/awslabs/aws-glue-schema-registry/pull/303)     | PR    | Use Apache HTTP client                               | Evaluate — HTTP-client family.                                                                                                                       |
| [#318](https://github.com/awslabs/aws-glue-schema-registry/pull/318)     | PR    | Tag-based schema version lookup                      | Evaluate, low priority.                                                                                                                              |
| [#320](https://github.com/awslabs/aws-glue-schema-registry/pull/320)     | PR    | JSON support for Java 8 date/times                   | Evaluate — with issue [#202].                                                                                                                        |
| [#321](https://github.com/awslabs/aws-glue-schema-registry/issues/321)   | issue | ObjectMapper customization                           | Evaluate — Jackson family, with [#325] and PR [#327].                                                                                                |
| [#325](https://github.com/awslabs/aws-glue-schema-registry/issues/325)   | issue | JsonDeserializer Jackson feature toggles             | Evaluate — verify the existing `jacksonDeserializationFeatures` key covers it.                                                                       |
| [#327](https://github.com/awslabs/aws-glue-schema-registry/pull/327)     | PR    | Override Jackson serde feature defaults              | Evaluate — verify the existing `jacksonSerializationFeatures` / `jacksonDeserializationFeatures` keys cover it; family of issues [#321] and [#325].  |
| [#341](https://github.com/awslabs/aws-glue-schema-registry/issues/341)   | issue | New AWSSchemaRegistryClient constructor              | Evaluate — with PR [#263].                                                                                                                           |
| [#347](https://github.com/awslabs/aws-glue-schema-registry/issues/347)   | issue | Nested protobuf message with oneof fails             | Evaluate — reproduce first.                                                                                                                          |
| [#368](https://github.com/awslabs/aws-glue-schema-registry/issues/368)   | issue | draft-07 not fully supported                         | Evaluate — JSON Schema converter family, with [#369], [#250] and PR [#527].                                                                          |
| [#369](https://github.com/awslabs/aws-glue-schema-registry/issues/369)   | issue | Converter fails on integer/number property           | Evaluate — same family.                                                                                                                              |
| [#371](https://github.com/awslabs/aws-glue-schema-registry/issues/371)   | issue | Dependency clash with newer Flink                    | Pending the Flink decision.                                                                                                                          |
| [#374](https://github.com/awslabs/aws-glue-schema-registry/pull/374)     | PR    | Bump Flink to 1.20.1                                 | Pending the Flink decision: migrate `avro-flink-serde` to 1.20 LTS or deprecate it. With issues [#291] and [#371].                                   |
| [#471](https://github.com/awslabs/aws-glue-schema-registry/issues/471)   | issue | Default schema naming collision                      | Evaluate — naming family, with PRs [#126] and [#472].                                                                                                |
| [#472](https://github.com/awslabs/aws-glue-schema-registry/pull/472)     | PR    | Differentiate schema name for keys                   | Evaluate — naming-collision family, with [#126], [#93], [#471].                                                                                      |
| [#491](https://github.com/awslabs/aws-glue-schema-registry/issues/491)   | issue | Incompatibility with protobuf-java 4.x               | Evaluate — the protobuf 3.x → 4.x edition jump is a deliberate separate step.                                                                        |
| [#509](https://github.com/awslabs/aws-glue-schema-registry/issues/509)   | issue | Upgrade the Kinesis Producer Library to 1.x          | Evaluate — a first move to 1.0.7 was reverted ([mobsuccess#115]) to keep the emulator-based integration suite working; retry deliberately.           |
| [#525](https://github.com/awslabs/aws-glue-schema-registry/pull/525)     | PR    | Move mbknor artifact to Scala 2.13                   | Evaluate — the fork attempted the 2.13 move and reverted it (`fb7e721`); retry deliberately.                                                         |
| [#527](https://github.com/awslabs/aws-glue-schema-registry/pull/527)     | PR    | JSON Schema `const` support in Connect converter     | Evaluate.                                                                                                                                            |
| [#534](https://github.com/awslabs/aws-glue-schema-registry/pull/534)     | PR    | Fix integration-test docker images                   | Planned — `integration-tests/docker-compose.yml` still pulls the removed bitnami images.                                                             |

## Addressed

13 pull requests, 28 issues.

| Upstream                                                                 | Type  | Title                                                    | Notes                                                                                                                                 |
| ------------------------------------------------------------------------ | ----- | -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| [#73](https://github.com/awslabs/aws-glue-schema-registry/issues/73)     | issue | Nullable fields in JSON Schema generation from POJOs     | [mobsuccess#117] (opt-in).                                                                                                               |
| [#128](https://github.com/awslabs/aws-glue-schema-registry/issues/128)   | issue | NPE when `avroRecordType` is absent                      | [mobsuccess#66] defaults it to `GENERIC_RECORD`.                                                                                         |
| [#136](https://github.com/awslabs/aws-glue-schema-registry/issues/136)   | issue | "Didn't find secondary deserializer" misfire             | Fixed at the fork point (upstream `#440`).                                                                                               |
| [#156](https://github.com/awslabs/aws-glue-schema-registry/issues/156)   | issue | Connect converter artifact is huge                       | Thin artifacts documented, compile scopes narrowed ([mobsuccess#89]); the uber-jar stays big by design for the Connect plugin path.      |
| [#186](https://github.com/awslabs/aws-glue-schema-registry/issues/186)   | issue | Protobuf POJO deserialization fails with Quarkus         | Classloader fix [mobsuccess#113].                                                                                                        |
| [#194](https://github.com/awslabs/aws-glue-schema-registry/issues/194)   | issue | Reduce the number of dependencies                        | Compile scopes narrowed to what each module exposes ([mobsuccess#89]); slimming continues.                                               |
| [#200](https://github.com/awslabs/aws-glue-schema-registry/issues/200)   | issue | Java 17                                                  | JVM 17 toolchain; suite runs on 17, 21, 25 ([mobsuccess#77], [mobsuccess#80]).                                                           |
| [#216](https://github.com/awslabs/aws-glue-schema-registry/pull/216)     | PR    | Fix optional fields in JSON                              | Superseded by the nullable-union handling shipped as [mobsuccess#116].                                                                   |
| [#218](https://github.com/awslabs/aws-glue-schema-registry/issues/218)   | issue | Deserializing optional union types                       | [mobsuccess#116].                                                                                                                        |
| [#252](https://github.com/awslabs/aws-glue-schema-registry/issues/252)   | issue | Cross-account access                                     | `assumeRoleArn` / `assumeRoleSessionName` shipped upstream in `#376`, present since the fork point and documented in the fork's docs.    |
| [#271](https://github.com/awslabs/aws-glue-schema-registry/issues/271)   | issue | Release with the merged wire-schema update               | Wire 6.4.6 ([mobsuccess#96]); the fork releases continuously.                                                                            |
| [#274](https://github.com/awslabs/aws-glue-schema-registry/issues/274)   | issue | AWS SDK v2 upgrade plans                                 | SDK v1 fully removed ([mobsuccess#72] and the Gradle port).                                                                              |
| [#275](https://github.com/awslabs/aws-glue-schema-registry/issues/275)   | issue | Schema converters: cache the schema object (10x)         | [mobsuccess#91].                                                                                                                         |
| [#281](https://github.com/awslabs/aws-glue-schema-registry/issues/281)   | issue | Protobuf POJO with Spring Boot                           | Classloader fix [mobsuccess#113].                                                                                                        |
| [#301](https://github.com/awslabs/aws-glue-schema-registry/issues/301)   | issue | Compatibility with kafka-clients 3                       | The fork builds against Kafka 3.9.2 ([mobsuccess#83]).                                                                                   |
| [#307](https://github.com/awslabs/aws-glue-schema-registry/issues/307)   | issue | Connect converter fails on null default values           | [mobsuccess#68].                                                                                                                         |
| [#308](https://github.com/awslabs/aws-glue-schema-registry/pull/308)     | PR    | Fix Avro record default values in Connect converter      | Ported as [mobsuccess#68].                                                                                                               |
| [#311](https://github.com/awslabs/aws-glue-schema-registry/issues/311)   | issue | Typed serde classes for Kafka Streams                    | Typed `Serde<T>` factories in the Kotlin module ([mobsuccess#125]).                                                                      |
| [#313](https://github.com/awslabs/aws-glue-schema-registry/issues/313)   | issue | kotlinx-serialization dependency conflict                | Catalog at kotlinx-serialization 1.11.0.                                                                                                 |
| [#336](https://github.com/awslabs/aws-glue-schema-registry/pull/336)     | PR    | Use the context classloader in protobuf deserialization  | Ported as [mobsuccess#113]; fixes the Spring Boot / Connect plugin-isolation failures ([#186], [#281], [#339], [#364]).                  |
| [#339](https://github.com/awslabs/aws-glue-schema-registry/issues/339)   | issue | Specific record read as generic / protobuf class lookup  | `avroRecordType` default ([mobsuccess#66]) and the classloader fix ([mobsuccess#113]).                                                   |
| [#350](https://github.com/awslabs/aws-glue-schema-registry/issues/350)   | issue | Release with updated kafka.scala.version                 | The fork's Kafka artifacts are unsuffixed; the remaining Scala-suffixed dependency is tracked via PR [#525].                             |
| [#353](https://github.com/awslabs/aws-glue-schema-registry/issues/353)   | issue | Unnecessary jimfs dependency                             | Removed in [mobsuccess#71].                                                                                                              |
| [#361](https://github.com/awslabs/aws-glue-schema-registry/issues/361)   | issue | Avro decimal to BigDecimal conversion error              | [mobsuccess#69].                                                                                                                         |
| [#363](https://github.com/awslabs/aws-glue-schema-registry/pull/363)     | PR    | Build with JDK 21                                        | JVM 17 toolchain; the suite runs on JDK 17, 21 and 25 in CI ([mobsuccess#77], [mobsuccess#80]).                                          |
| [#364](https://github.com/awslabs/aws-glue-schema-registry/issues/364)   | issue | ProtobufSchemaLoader fails in a Spring repackaged JAR    | Classloader fix [mobsuccess#113].                                                                                                        |
| [#372](https://github.com/awslabs/aws-glue-schema-registry/issues/372)   | issue | Wire CVE-2024-58103 (nested-group recursion)             | Wire 6.4.6 ([mobsuccess#96]).                                                                                                            |
| [#380](https://github.com/awslabs/aws-glue-schema-registry/issues/380)   | issue | Vulnerable dependency updates                            | Dependabot plus the audit's dependency work (Kafka 3.9.2, Wire 6.4.6, log4j 2.26, Guava 33.6, …).                                        |
| [#477](https://github.com/awslabs/aws-glue-schema-registry/pull/477)     | PR    | Java 17 LTS build compatibility                          | Same coverage as [#363].                                                                                                                 |
| [#478](https://github.com/awslabs/aws-glue-schema-registry/issues/478)   | issue | Avro serde performance (Connect & Flink)                 | [mobsuccess#91].                                                                                                                         |
| [#494](https://github.com/awslabs/aws-glue-schema-registry/pull/494)     | PR    | Client-side JSON schema compatibility check              | Ported as [mobsuccess#119] (opt-in).                                                                                                     |
| [#500](https://github.com/awslabs/aws-glue-schema-registry/issues/500)   | issue | Completely remove AWS SDK v1                             | [mobsuccess#72] and the Gradle port; no v1 reference remains.                                                                            |
| [#511](https://github.com/awslabs/aws-glue-schema-registry/issues/511)   | issue | Build fails on JDK 25 (Lombok)                           | The Kotlin conversion removed the problem at the root; the suite runs on JDK 17/21/25 in CI.                                             |
| [#522](https://github.com/awslabs/aws-glue-schema-registry/issues/522)   | issue | Wire CVE-2026-45799 (wire-runtime 6.3+)                  | Wire 6.4.6 ([mobsuccess#96]).                                                                                                            |
| [#526](https://github.com/awslabs/aws-glue-schema-registry/pull/526)     | PR    | Fix deserialization of optional union types (anyOf null) | Ported as [mobsuccess#116]; fixes issue [#218].                                                                                          |
| [#528](https://github.com/awslabs/aws-glue-schema-registry/pull/528)     | PR    | Cache parsed Avro Schema (performance)                   | Ported as [mobsuccess#91]; fixes issues [#478] and [#275].                                                                               |
| [#529](https://github.com/awslabs/aws-glue-schema-registry/pull/529)     | PR    | Nullable fields in JSON Schema generation                | Ported as [mobsuccess#117] (opt-in); fixes issue [#73].                                                                                  |
| [#530](https://github.com/awslabs/aws-glue-schema-registry/pull/530)     | PR    | Bump kotlinx-serialization to 1.7.3                      | The fork's catalog is at kotlinx-serialization 1.11.0.                                                                                   |
| [#531](https://github.com/awslabs/aws-glue-schema-registry/pull/531)     | PR    | Opt-out for className-based deserialization              | Superseded by the allowlist model of upstream `#533`, in the fork since 2.0.0 (`className` resolution off by default).                   |
| [#532](https://github.com/awslabs/aws-glue-schema-registry/pull/532)     | PR    | Handle protobuf STRUCT without metadata                  | Ported as [mobsuccess#67], which also covers null schema names.                                                                          |
| [#536](https://github.com/awslabs/aws-glue-schema-registry/pull/536)     | PR    | Pin Actions to SHAs, Dependabot, zizmor                  | The fork's CI is SHA-pinned with its own hardening ([mobsuccess#79], [mobsuccess#82]).                                                   |

## Out of scope

20 pull requests, 48 issues.

| Upstream                                                                 | Type  | Title                                                | Reason                                                                                                             |
| ------------------------------------------------------------------------ | ----- | ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| [#28](https://github.com/awslabs/aws-glue-schema-registry/issues/28)     | issue | More examples with regular Kafka tools               | Stale documentation ask; the fork's docs were rewritten per integration.                                              |
| [#35](https://github.com/awslabs/aws-glue-schema-registry/issues/35)     | issue | Cache schemas on the filesystem                      | Niche; no demand since 2021.                                                                                          |
| [#36](https://github.com/awslabs/aws-glue-schema-registry/issues/36)     | issue | Consume records on the CLI                           | Tooling outside the serde libraries.                                                                                  |
| [#43](https://github.com/awslabs/aws-glue-schema-registry/issues/43)     | issue | Multi-language support                               | Multi-language layer dropped.                                                                                         |
| [#48](https://github.com/awslabs/aws-glue-schema-registry/issues/48)     | issue | Usage with MSK / from PHP                            | Usage question.                                                                                                       |
| [#69](https://github.com/awslabs/aws-glue-schema-registry/issues/69)     | issue | AWS signature for authentication                     | Stale research question.                                                                                              |
| [#95](https://github.com/awslabs/aws-glue-schema-registry/issues/95)     | issue | Apache Druid client                                  | Usage question.                                                                                                       |
| [#96](https://github.com/awslabs/aws-glue-schema-registry/issues/96)     | issue | Kafka Connect dependencies question                  | Question; the fork's docs cover the shaded-vs-thin choice.                                                            |
| [#137](https://github.com/awslabs/aws-glue-schema-registry/issues/137)   | issue | Docker setup question                                | Stale support question.                                                                                               |
| [#151](https://github.com/awslabs/aws-glue-schema-registry/issues/151)   | issue | AccessDenied on Kubernetes                           | Environment support.                                                                                                  |
| [#154](https://github.com/awslabs/aws-glue-schema-registry/issues/154)   | issue | Converter metadata documentation                     | Upstream documentation.                                                                                               |
| [#174](https://github.com/awslabs/aws-glue-schema-registry/issues/174)   | issue | Registering a version does not move the checkpoint   | AWS Glue service behaviour, not the library.                                                                          |
| [#175](https://github.com/awslabs/aws-glue-schema-registry/issues/175)   | issue | Serializer does not update the checkpoint            | Same as [#174].                                                                                                       |
| [#180](https://github.com/awslabs/aws-glue-schema-registry/issues/180)   | issue | Warnings for Map types                               | Stale, no reproduction.                                                                                               |
| [#197](https://github.com/awslabs/aws-glue-schema-registry/issues/197)   | issue | Spark Structured Streaming                           | Usage question.                                                                                                       |
| [#207](https://github.com/awslabs/aws-glue-schema-registry/issues/207)   | issue | Glue with MSK                                        | Usage question.                                                                                                       |
| [#208](https://github.com/awslabs/aws-glue-schema-registry/issues/208)   | issue | ClassNotFoundException for JacksonUtils              | Stale environment issue.                                                                                              |
| [#212](https://github.com/awslabs/aws-glue-schema-registry/issues/212)   | issue | AVROUtils "Unsupported data type"                    | Usage question.                                                                                                       |
| [#213](https://github.com/awslabs/aws-glue-schema-registry/issues/213)   | issue | Build fails in Dockerfile                            | Maven-era; the fork builds with Gradle.                                                                               |
| [#215](https://github.com/awslabs/aws-glue-schema-registry/issues/215)   | issue | Multiple event types in the same topic               | Usage question; the underlying gap is the naming family ([#93]).                                                      |
| [#226](https://github.com/awslabs/aws-glue-schema-registry/issues/226)   | issue | Extending Confluent's Connect docker images          | Stale environment issue.                                                                                              |
| [#259](https://github.com/awslabs/aws-glue-schema-registry/issues/259)   | issue | Failed to get schema version id                      | Stale support question.                                                                                               |
| [#260](https://github.com/awslabs/aws-glue-schema-registry/issues/260)   | issue | Fails with FilePulse connector                       | Stale, environment-specific.                                                                                          |
| [#261](https://github.com/awslabs/aws-glue-schema-registry/issues/261)   | issue | Fails to parse a MAP attribute                       | Upstream marked it wontfix.                                                                                           |
| [#264](https://github.com/awslabs/aws-glue-schema-registry/issues/264)   | issue | Spring Cloud SchemaRegistryClient                    | Integration outside the serde.                                                                                        |
| [#265](https://github.com/awslabs/aws-glue-schema-registry/issues/265)   | issue | Authentication on MSK Connect                        | Environment support.                                                                                                  |
| [#267](https://github.com/awslabs/aws-glue-schema-registry/issues/267)   | issue | Unable to find suppressions file                     | Maven/checkstyle relic; the fork lints with ktlint.                                                                   |
| [#268](https://github.com/awslabs/aws-glue-schema-registry/issues/268)   | issue | JSON evaluation fails for a source connector         | Stale support question.                                                                                               |
| [#272](https://github.com/awslabs/aws-glue-schema-registry/issues/272)   | issue | Reading Avro messages fails                          | Stale support question.                                                                                               |
| [#282](https://github.com/awslabs/aws-glue-schema-registry/issues/282)   | issue | JavaScript / Node.js support                         | Outside the JVM scope.                                                                                                |
| [#288](https://github.com/awslabs/aws-glue-schema-registry/issues/288)   | issue | IRSA role not used on EKS                            | Environment support; see [#293] for pluggable credentials.                                                            |
| [#292](https://github.com/awslabs/aws-glue-schema-registry/issues/292)   | issue | Remove cross-replication configurations              | Upstream-internal cleanup.                                                                                            |
| [#294](https://github.com/awslabs/aws-glue-schema-registry/issues/294)   | issue | Prefix topic name with source cluster alias          | Replication family ([#352]).                                                                                          |
| [#305](https://github.com/awslabs/aws-glue-schema-registry/issues/305)   | issue | Migrate to json-sKema for validation                 | Swapping the validator changes observable behaviour; revisit only if everit stalls.                                   |
| [#306](https://github.com/awslabs/aws-glue-schema-registry/issues/306)   | issue | GraalVM native-image support                         | Reflection-heavy dependencies make it a research project.                                                             |
| [#309](https://github.com/awslabs/aws-glue-schema-registry/issues/309)   | issue | Deserialized object without properties               | Stale support question.                                                                                               |
| [#315](https://github.com/awslabs/aws-glue-schema-registry/pull/315)     | PR    | Append 2.12 suffix to artifactIds                    | Maven artifact naming; the fork's coordinates are unsuffixed. The Scala-suffixed _dependency_ question is [#525].     |
| [#316](https://github.com/awslabs/aws-glue-schema-registry/issues/316)   | issue | NonRecordContainer serialization                     | Usage question.                                                                                                       |
| [#326](https://github.com/awslabs/aws-glue-schema-registry/issues/326)   | issue | Avro deserialization fails in 1.1.18                 | Stale, version-specific.                                                                                              |
| [#329](https://github.com/awslabs/aws-glue-schema-registry/pull/329)     | PR    | Bump commons-compress (avro-flink-serde)             | The fork manages versions in its own catalog with Dependabot, and excludes commons-compress from the Flink module.    |
| [#330](https://github.com/awslabs/aws-glue-schema-registry/pull/330)     | PR    | Bump commons-compress                                | Same as [#329].                                                                                                       |
| [#340](https://github.com/awslabs/aws-glue-schema-registry/issues/340)   | issue | BACKWARD compatibility in the S3 sink                | Environment support.                                                                                                  |
| [#349](https://github.com/awslabs/aws-glue-schema-registry/issues/349)   | issue | Why is Kafka Streams caching disabled in the example | Question; candidate for a docs note.                                                                                  |
| [#352](https://github.com/awslabs/aws-glue-schema-registry/pull/352)     | PR    | Schema replication converter                         | Cross-cluster replication is outside the serde scope of this fork.                                                    |
| [#354](https://github.com/awslabs/aws-glue-schema-registry/issues/354)   | issue | Why pass the schema to the Flink constructor         | Usage question.                                                                                                       |
| [#367](https://github.com/awslabs/aws-glue-schema-registry/issues/367)   | issue | Unit tests failing                                   | Upstream build environment; the fork's suite is green on JDK 17/21/25 in CI, and the integration suite runs nightly.  |
| [#370](https://github.com/awslabs/aws-glue-schema-registry/issues/370)   | issue | Run without logging into AWS                         | The registry is an AWS service; a no-auth mode is a different product.                                                |
| [#373](https://github.com/awslabs/aws-glue-schema-registry/issues/373)   | issue | Automation analysis                                  | Bot noise.                                                                                                            |
| [#375](https://github.com/awslabs/aws-glue-schema-registry/issues/375)   | issue | Versioning question on the deserializer              | Usage question.                                                                                                       |
| [#378](https://github.com/awslabs/aws-glue-schema-registry/issues/378)   | issue | mvn clean install does not work                      | Maven removed; the fork builds with Gradle.                                                                           |
| [#379](https://github.com/awslabs/aws-glue-schema-registry/pull/379)     | PR    | Safer bot POM updates                                | Automated Maven-POM bot; the fork is Gradle with Dependabot.                                                          |
| [#419](https://github.com/awslabs/aws-glue-schema-registry/pull/419)     | PR    | Add Golang build workflow                            | Go / multi-language layer dropped.                                                                                    |
| [#423](https://github.com/awslabs/aws-glue-schema-registry/pull/423)     | PR    | Negative integration tests for configuration paths   | Go integration-test suite; multi-language layer dropped.                                                              |
| [#441](https://github.com/awslabs/aws-glue-schema-registry/pull/441)     | PR    | Deserializer middleware for parallel consumers       | C# (KafkaFlow); C# dropped.                                                                                           |
| [#447](https://github.com/awslabs/aws-glue-schema-registry/pull/447)     | PR    | JSON Schema test resources                           | Native-module test data; native layer dropped.                                                                        |
| [#458](https://github.com/awslabs/aws-glue-schema-registry/pull/458)     | PR    | Null check in C# dispose flow                        | C# dropped.                                                                                                           |
| [#463](https://github.com/awslabs/aws-glue-schema-registry/pull/463)     | PR    | Migrate to the Central Publishing Maven plugin       | The fork publishes with Gradle to GitHub Packages; Maven Central is deliberately parked for now.                      |
| [#466](https://github.com/awslabs/aws-glue-schema-registry/pull/466)     | PR    | C# protobuf/avro evolution tests                     | C# dropped.                                                                                                           |
| [#468](https://github.com/awslabs/aws-glue-schema-registry/pull/468)     | PR    | Golang Avro evolution tests                          | Go dropped.                                                                                                           |
| [#469](https://github.com/awslabs/aws-glue-schema-registry/pull/469)     | PR    | SerDes lifecycle memory-leak test with ASan          | Native C layer dropped.                                                                                               |
| [#473](https://github.com/awslabs/aws-glue-schema-registry/pull/473)     | PR    | C# IAM policy integration tests                      | C# dropped.                                                                                                           |
| [#474](https://github.com/awslabs/aws-glue-schema-registry/pull/474)     | PR    | Drop redundant C# validation                         | C# dropped.                                                                                                           |
| [#479](https://github.com/awslabs/aws-glue-schema-registry/pull/479)     | PR    | Native unicode tests (C#)                            | C# dropped.                                                                                                           |
| [#483](https://github.com/awslabs/aws-glue-schema-registry/pull/483)     | PR    | C# Kinesis Client Library integration                | C# dropped.                                                                                                           |
| [#487](https://github.com/awslabs/aws-glue-schema-registry/pull/487)     | PR    | C# nuget package metadata                            | C# dropped.                                                                                                           |
| [#492](https://github.com/awslabs/aws-glue-schema-registry/issues/492)   | issue | C# proxy certificate issue                           | C# dropped.                                                                                                           |
| [#493](https://github.com/awslabs/aws-glue-schema-registry/issues/493)   | issue | Unable to load GsrSerDeCsGen shared library          | Native layer dropped.                                                                                                 |
| [#495](https://github.com/awslabs/aws-glue-schema-registry/pull/495)     | PR    | README: reference AWS Connectors                     | Upstream documentation; the fork's README was rewritten.                                                              |

[#32]: https://github.com/awslabs/aws-glue-schema-registry/issues/32
[#93]: https://github.com/awslabs/aws-glue-schema-registry/issues/93
[#120]: https://github.com/awslabs/aws-glue-schema-registry/issues/120
[#126]: https://github.com/awslabs/aws-glue-schema-registry/pull/126
[#174]: https://github.com/awslabs/aws-glue-schema-registry/issues/174
[#186]: https://github.com/awslabs/aws-glue-schema-registry/issues/186
[#198]: https://github.com/awslabs/aws-glue-schema-registry/issues/198
[#202]: https://github.com/awslabs/aws-glue-schema-registry/issues/202
[#218]: https://github.com/awslabs/aws-glue-schema-registry/issues/218
[#240]: https://github.com/awslabs/aws-glue-schema-registry/pull/240
[#250]: https://github.com/awslabs/aws-glue-schema-registry/issues/250
[#263]: https://github.com/awslabs/aws-glue-schema-registry/pull/263
[#275]: https://github.com/awslabs/aws-glue-schema-registry/issues/275
[#281]: https://github.com/awslabs/aws-glue-schema-registry/issues/281
[#291]: https://github.com/awslabs/aws-glue-schema-registry/issues/291
[#293]: https://github.com/awslabs/aws-glue-schema-registry/issues/293
[#303]: https://github.com/awslabs/aws-glue-schema-registry/pull/303
[#320]: https://github.com/awslabs/aws-glue-schema-registry/pull/320
[#321]: https://github.com/awslabs/aws-glue-schema-registry/issues/321
[#325]: https://github.com/awslabs/aws-glue-schema-registry/issues/325
[#329]: https://github.com/awslabs/aws-glue-schema-registry/pull/329
[#339]: https://github.com/awslabs/aws-glue-schema-registry/issues/339
[#341]: https://github.com/awslabs/aws-glue-schema-registry/issues/341
[#352]: https://github.com/awslabs/aws-glue-schema-registry/pull/352
[#363]: https://github.com/awslabs/aws-glue-schema-registry/pull/363
[#364]: https://github.com/awslabs/aws-glue-schema-registry/issues/364
[#368]: https://github.com/awslabs/aws-glue-schema-registry/issues/368
[#369]: https://github.com/awslabs/aws-glue-schema-registry/issues/369
[#371]: https://github.com/awslabs/aws-glue-schema-registry/issues/371
[#374]: https://github.com/awslabs/aws-glue-schema-registry/pull/374
[#471]: https://github.com/awslabs/aws-glue-schema-registry/issues/471
[#472]: https://github.com/awslabs/aws-glue-schema-registry/pull/472
[#478]: https://github.com/awslabs/aws-glue-schema-registry/issues/478
[#525]: https://github.com/awslabs/aws-glue-schema-registry/pull/525
[#527]: https://github.com/awslabs/aws-glue-schema-registry/pull/527
[mobsuccess#66]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/66
[mobsuccess#67]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/67
[mobsuccess#68]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/68
[mobsuccess#69]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/69
[mobsuccess#71]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/71
[mobsuccess#72]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/72
[mobsuccess#77]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/77
[mobsuccess#79]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/79
[mobsuccess#80]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/80
[mobsuccess#81]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/81
[mobsuccess#82]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/82
[mobsuccess#83]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/83
[mobsuccess#89]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/89
[mobsuccess#91]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/91
[mobsuccess#94]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/94
[mobsuccess#96]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/96
[mobsuccess#113]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/113
[mobsuccess#115]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/115
[mobsuccess#116]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/116
[mobsuccess#117]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/117
[mobsuccess#119]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/119
[mobsuccess#125]: https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/125
