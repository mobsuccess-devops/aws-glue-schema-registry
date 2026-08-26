import org.gradle.api.provider.Property

interface GsrCoverageExtension {
    val enabled: Property<Boolean>
    val minimumInstructionCoverage: Property<Double>
    val minimumBranchCoverage: Property<Double>
}
