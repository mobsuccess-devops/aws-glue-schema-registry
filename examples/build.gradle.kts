plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

dependencies {
    implementation(project(":schema-registry-serde"))
    implementation(platform(libs.aws.bom))
    implementation(platform(libs.jackson.bom))
    implementation(libs.aws.kinesis)
    implementation(libs.jackson.cbor)
    implementation(libs.commons.cli)
}

tasks.register<JavaExec>("runExample") {
    group = "application"
    description = "Runs PutRecordGetRecordExample against real Kinesis and Glue resources."
    mainClass.set("com.amazonaws.services.schemaregistry.examples.kds.PutRecordGetRecordExample")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = projectDir
}
