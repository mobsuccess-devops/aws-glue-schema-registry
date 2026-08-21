# Kotlin conversion and Java interop

The fork converts the inherited Java sources to Kotlin without changing behaviour. This
page holds the method and the traps met along the way; the contract itself is in
[portage.md](portage.md).

## Conversion method

Kotlin and Java compile together within a module. `main` sources are therefore converted
**while the tests stay in Java**: the inherited suite has not moved and acts as an oracle
for the converted code. Tests are converted in a second pass, once all of `main` is done.

Work module by module, in dependency order, and run the **whole** build before committing:
a module's own tests do not cover the modules that consume it.

## Java interop traps

These all cost a red test at least once. They are listed in the order they bite.

- **Kotlin classes and methods are final by default**, unlike their Java counterparts. Any
  type a test mocks needs `open` on the class _and_ on every stubbed method.
- **`@NonNull` raised an `IllegalArgumentException`**, not a `NullPointerException`, because
  of `lombok.nonNull.exceptionType` in `lombok.config`. Converting to a non-nullable type
  changes the exception type; update the asserting test rather than weakening the signature.
- **Kotlin does not see Lombok-generated accessors** on a Java class it compiles alongside; it
  resolves the property name to the private field instead. The Kotlin Lombok plugin used to
  fix that in the conventions. It is gone: no Kotlin source consumes Lombok any more. Bring it
  back only if Lombok reappears in a module that also holds Kotlin.
- **Lombok's `@Builder` has no Kotlin equivalent.** Rewrite it as a nested `Builder` class
  plus a `@JvmStatic builder()`, so the API seen from Java stays identical.
- **`@Data` also generated `equals`/`hashCode`/`toString`.** Omitting them silently falls
  back to identity comparison.
- **Boolean accessors:** Lombok generates `isXxx()` for a `boolean xxx` field; Kotlin
  generates `getXxx()` unless the property itself is named `isXxx`.
- **Enums cannot redeclare `name`.** Rename the backing property and expose `getName()`.
- **Checked exceptions vanish** without `@Throws`, and Java callers catching them stop
  compiling.
- **A Kotlin `inner` class cannot hold a companion object.** Move its constants to the outer
  companion.
- **Private functions get no parameter null checks**, unlike public ones.
- **Kotlin does not widen `int` to `long` implicitly**, nor infer generic variance the way
  javac did.
- **A cast can be optimized away.** `(value as CharSequence).toString()` resolves `toString()`
  on `Any`, so the checkcast is elided and a wrong-typed value is silently accepted where the
  Java code threw `ClassCastException`. Bind the cast to a typed local when the cast itself is
  the check.
- **Collection ordering is observable.** `HashSet`/`HashMap` iteration order ends up in
  rendered JSON and in schema equality; replacing them with Kotlin's order-preserving `setOf`
  or `mapOf` changes the output.
- **The order of `instanceof` branches matters** when the types are related — `EnumSchema`
  extends `StringSchema`, so reordering a `when` changes which converter is selected.
- **`String.split` does not drop trailing empty parts** the way Java's does. `"a/b/".split("/")`
  yields three elements in Kotlin and two in Java; append `.dropLastWhile { it.isEmpty() }`
  wherever the Java code relied on that trimming.
- **`internal` members of a Kotlin dependency are unreachable.** Wire's `ProtoParser`
  constructor is `internal`: Java saw it as public, Kotlin does not. Look for the public
  entry point that wraps it rather than working around the visibility.
- **Java's package-private and `protected` nested types have no Kotlin equivalent** when a
  public method exposes them. Make the class public with an `internal` constructor.
- **A test cannot hand a literal `null` to a non-nullable parameter.** The tests that assert
  a null is rejected have to route it through an erased generic — `nullOf()` in
  `TestNulls.kt` — so the check that fires is still the callee's own, not one the test
  performed on its behalf.
- **`@MethodSource` resolves by JVM name.** A provider must be `@JvmStatic` in a companion
  object and must not be `internal`: name mangling makes JUnit report the method as missing.
- **`Mockito.any(Foo.class)` returns null**, which a Kotlin non-nullable parameter rejects
  before the mock is ever reached. Use mockito-kotlin's `any<Foo>()`, which hands back a
  non-null stand-in; the same applies to `anyMap()` and `anyString()`.
