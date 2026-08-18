# Portage Maven → Gradle → Kotlin

Ce fork part du commit `eed1506` de `awslabs/aws-glue-schema-registry`, poussé tel quel
comme premier commit. Tout écart est donc lisible avec :

```bash
git diff eed1506 -- <chemin>
```

## Contrat d'iso

Le portage se fait en deux temps volontairement séparés — d'abord le système de build,
ensuite le langage — pour qu'un test rouge n'ait jamais deux causes possibles.

La référence est la suite de tests du repo source, mesurée sous Maven avant toute
modification (JDK 17, `mvn test -pl '!integration-tests'`) :

| Module                              | Tests    |
| ----------------------------------- | -------- |
| `common`                            | 139      |
| `serializer-deserializer`           | 1346     |
| `kafkastreams-serde`                | 22       |
| `avro-kafkaconnect-converter`       | 22       |
| `avro-flink-serde`                  | 20       |
| `jsonschema-kafkaconnect-converter` | 329      |
| `protobuf-kafkaconnect-converter`   | 95       |
| **Total**                           | **1973** |

Le build Gradle reproduit ces 1973 tests à l'identique, module par module, sans échec.
Ce total est le seuil à retrouver après chaque étape de conversion Kotlin.

## Écarts assumés par rapport au build Maven

- **C# et `multilang-schema-registry` retirés.** La partie Java du module n'existait que
  pour exposer une bibliothèque native au binding C# ; sans consommateur, elle n'a plus
  d'objet.
- **`build-tools` retiré.** Ce module ne portait que la configuration Checkstyle du build
  Maven, remplacée par ktlint.
- **114 tests JUnit 4 réveillés.** `AvroDataTest` (105) et `AdditionalAvroDataTest` (9)
  n'étaient exécutés par aucun moteur sous Maven, faute de `junit-vintage-engine` : ils
  étaient silencieusement ignorés. Ils sont désormais lancés et passent tous, ce qui porte
  `avro-kafkaconnect-converter` de 22 à 136 tests. Le code testé n'a pas changé.
- **`avro-flink-serde` pointe vers le module local.** Le pom dépendait de
  `schema-registry-serde` publié sur Maven Central (2.0.0 en compile, 1.0.2 en test) au
  lieu du module voisin.
- **`org.lz4:lz4-java` exclu globalement.** Le pom l'excluait de chaque artefact Kafka au
  profit du fork `at.yawk.lz4:lz4-java`. Les deux déclarent la même _capability_, que
  Gradle refuse d'arbitrer seul.
- **`@NonNull` : `IllegalArgumentException` devient `NullPointerException`.** Le
  `lombok.config` du dépôt fixe `lombok.nonNull.exceptionType = IllegalArgumentException`,
  si bien que les 112 `@NonNull` levaient une `IllegalArgumentException` sur argument nul.
  Les types non-nullables de Kotlin lèvent un `NullPointerException`. Le choix a été fait
  de garder du Kotlin idiomatique plutôt que des paramètres nullables validés par
  `require()`, au prix de la mise à jour des tests qui vérifiaient le type d'exception.
  Seul le type change : une valeur nulle est toujours refusée, au même endroit.
- **Coordonnées de publication.** Groupe `com.mobsuccess` au lieu de
  `software.amazon.glue`, pour qu'un artefact de ce fork ne puisse pas se substituer
  silencieusement à celui de Maven Central chez un consommateur. Les `artifactId` sont
  inchangés.
