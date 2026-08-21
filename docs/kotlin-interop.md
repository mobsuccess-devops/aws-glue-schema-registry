# Java interop

The fork's promise is an API and a behaviour identical to the Java source. Kotlin gives
that away in places where the two languages do not line up, and each rule below cost a red
test at least once. The port's own history — how the conversion was sequenced, and what it
deliberately changed — is in [portage.md](portage.md).

## Keeping the API Java-shaped

The published surface is consumed from Java, and `apiCheck` fails on any signature that
moves. These are the differences that silently reshape it.

- **Kotlin classes and methods are final by default**, unlike their Java counterparts. Any
  type a test mocks needs `open` on the class _and_ on every stubbed method.
- **Checked exceptions vanish** without `@Throws`, and Java callers catching them stop
  compiling.
- **Boolean accessors:** Lombok generates `isXxx()` for a `boolean xxx` field; Kotlin
  generates `getXxx()` unless the property itself is named `isXxx`.
- **Enums cannot redeclare `name`.** Rename the backing property and expose `getName()`.
- **Java's package-private and `protected` nested types have no Kotlin equivalent** when a
  public method exposes them. Make the class public with an `internal` constructor.
- **Lombok's `@Builder` has no Kotlin equivalent.** Rewrite it as a nested `Builder` class
  plus a `@JvmStatic builder()`, so the API seen from Java stays identical.

## Behaviour that changes without a compiler error

The dangerous half: the build stays green and the output moves.

- **A cast can be optimized away.** `(value as CharSequence).toString()` resolves
  `toString()` on `Any`, so the checkcast is elided and a wrong-typed value is silently
  accepted where the Java code threw `ClassCastException`. Bind the cast to a typed local
  when the cast itself is the check.
- **Collection ordering is observable.** `HashSet`/`HashMap` iteration order ends up in
  rendered JSON and in schema equality; replacing them with Kotlin's order-preserving
  `setOf` or `mapOf` changes the output.
- **The order of `instanceof` branches matters** when the types are related — `EnumSchema`
  extends `StringSchema`, so reordering a `when` changes which converter is selected.
- **`String.split` does not drop trailing empty parts** the way Java's does. `"a/b/".split("/")`
  yields three elements in Kotlin and two in Java; append `.dropLastWhile { it.isEmpty() }`
  wherever the Java code relied on that trimming.
- **`@Data` also generated `equals`/`hashCode`/`toString`.** Omitting them silently falls
  back to identity comparison.

## Writing tests in Kotlin

- **`@MethodSource` resolves by JVM name.** A provider must be `@JvmStatic` in a companion
  object and must not be `internal`: name mangling makes JUnit report the method as missing.
- **`Mockito.any(Foo.class)` returns null**, which a Kotlin non-nullable parameter rejects
  before the mock is ever reached. Use mockito-kotlin's `any<Foo>()`, which hands back a
  non-null stand-in; the same applies to `anyMap()` and `anyString()`.
- **A test cannot hand a literal `null` to a non-nullable parameter.** The tests that assert
  a null is rejected have to route it through an erased generic — `nullOf()` in
  `TestNulls.kt` — so the check that fires is still the callee's own, not one the test
  performed on its behalf.

## The Java that remains

Every module is Kotlin. The only Java left is the Avro classes generated into the test
trees — `Person` in `integration-tests`, `User` in three others — which are regenerated
from their `.avsc` rather than edited, and Kotlin compiles alongside them without trouble.

Lombok is gone with the last hand-written Java. Two of its behaviours had to be reproduced
by hand when `integration-tests` was converted, and are worth knowing if any of it comes
back:

- **`@Builder` has no Kotlin equivalent**, as above: `Car`, `ConsumerProperties` and
  `ProducerProperties` carry a hand-written nested `Builder` plus a `@JvmStatic builder()`.
- **`@EqualsAndHashCode` compares arrays deeply.** `Car` holds a `String[]`, and the JSON
  specific-record tests compare a produced record against the one read back. A Kotlin
  `equals` that compares that array by identity turns those tests red; `Car.equals` uses
  `Arrays.deepEquals`, as Lombok did.
