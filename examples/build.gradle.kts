plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

dependencies {
    api(project(":schema-registry-serde"))
    api(platform(libs.aws.bom))
    api(platform(libs.jackson.bom))
    api(libs.aws.kinesis)
    api(libs.jackson.cbor)
    api(libs.commons.cli)
}

tasks.register<JavaExec>("runExample") {
    group = "application"
    description = "Runs PutRecordGetRecordExample against real Kinesis and Glue resources."
    mainClass.set("com.amazonaws.services.schemaregistry.examples.kds.PutRecordGetRecordExample")
    classpath = sourceSets["main"].runtimeClasspath
    workingDir = projectDir
}
