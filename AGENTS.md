# aws-glue-schema-registry

Fork Mobsuccess de [`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry),
réduit à la partie Java et porté sur Gradle. La conversion du code vers Kotlin est en cours.

## État du portage

| Étape                            | Statut  |
| -------------------------------- | ------- |
| Retrait du C# et du module natif | fait    |
| Migration Maven → Gradle         | fait    |
| Conversion Java → Kotlin         | à faire |

Le premier commit du repo est le source `awslabs` à l'identique (`eed1506`). Tout écart se
lit avec `git diff eed1506 -- <chemin>`. Les écarts assumés sont listés dans
[docs/portage.md](docs/portage.md).

## Règle d'or

Le repo doit rester **iso au repo source au niveau du comportement Java**. La suite de
tests héritée est le seul garde-fou : **2087 tests, zéro échec**. Une étape de conversion
qui fait baisser ce total ou casser un test n'est pas terminée, quelle que soit la
qualité apparente du code produit.

```bash
./gradlew clean build     # compile + 2087 tests + jars
./gradlew test            # tests seuls
./gradlew assemble        # jars seuls
```

## Structure

Dix modules, dont les répertoires reprennent ceux du repo source et dont les noms de
projet Gradle reprennent les `artifactId` Maven :

| Répertoire                          | Artefact                                 | Rôle                                     |
| ----------------------------------- | ---------------------------------------- | ---------------------------------------- |
| `common`                            | `schema-registry-common`                 | client Glue, cache, exceptions           |
| `serializer-deserializer`           | `schema-registry-serde`                  | cœur SerDe (Avro, JSON Schema, Protobuf) |
| `serializer-deserializer-msk-iam`   | `schema-registry-serde-msk-iam`          | uber-jar SerDe + auth IAM MSK            |
| `kafkastreams-serde`                | `schema-registry-kafkastreams-serde`     | intégration Kafka Streams                |
| `avro-kafkaconnect-converter`       | `schema-registry-kafkaconnect-converter` | converter Connect Avro                   |
| `avro-flink-serde`                  | `schema-registry-flink-serde`            | schémas de (dé)sérialisation Flink       |
| `jsonschema-kafkaconnect-converter` | `jsonschema-kafkaconnect-converter`      | converter Connect JSON Schema            |
| `protobuf-kafkaconnect-converter`   | `protobuf-kafkaconnect-converter`        | converter Connect Protobuf               |
| `examples`                          | `schema-registry-examples`               | exemples d'intégration                   |
| `integration-tests`                 | `schema-registry-integration-tests`      | tests exigeant de vraies ressources AWS  |

Le graphe est linéaire : `common` → `serializer-deserializer` → tous les autres. C'est
l'ordre à suivre pour la conversion Kotlin.

## Build

- Gradle 9.6.1, Kotlin DSL, toolchain **JVM 17** (consommable par Kafka Connect et Flink)
- Versions centralisées dans `gradle/libs.versions.toml` — ne jamais écrire une version en
  dur dans un `build.gradle.kts`
- Conventions communes dans `buildSrc/src/main/kotlin/gsr.*.gradle.kts`, pas de
  `subprojects {}` dans le build racine
- Publication sur GitHub Packages, groupe `com.mobsuccess.glue`

### Pièges connus

- **Lombok** est encore actif tant que le code est en Java. Chaque classe convertie en
  Kotlin doit perdre ses annotations Lombok au profit des équivalents natifs.
- **`org.lz4:lz4-java` est exclu globalement** au profit de `at.yawk.lz4:lz4-java`. Les
  deux déclarent la même _capability_ ; réintroduire le premier casse la résolution.
- **Génération de code** : protobuf (`serializer-deserializer`,
  `protobuf-kafkaconnect-converter`) et Avro (`avro-kafkaconnect-converter`). Les sources
  générées ne sont pas versionnées.
- **`serializer-deserializer` publie un jar `tests`** consommé par `integration-tests` via
  la configuration `testArtifacts`.
- Les dépendances Kotlin tirées par `mbknor-jackson-jsonschema` et `wire` sont figées en
  `1.9.25` (`kotlinRuntime` dans le catalogue) : c'est distinct de la version du compilateur
  Kotlin qu'utilisera le code converti.

## Conventions

- Lint Kotlin : ktlint 1.4.1, configuré dans `.editorconfig`, style `intellij_idea`
- Hooks locaux : `pre-commit install` (prettier, ktlint, fins de fichier)
- Commits et PR en français, une PR par module converti
- `.mobsuccess.yml` désactive les workflows `linear`, `ms-testers`, `mobsuccess`, `closed`
  et `python` : ce repo n'exige pas de ticket Linear par PR.

## Tests d'intégration

Le module `integration-tests` exige de vraies ressources AWS. Ses classes `*IntegrationTest`
sont exclues du run unitaire par la convention de build — ne pas les réactiver en CI.
