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

// The sample reads src/main/resources/user.avsc through a path relative to the working
// directory, which JavaExec sets to the project directory.
tasks.register<JavaExec>("runExample") {
    group = "application"
    description = "Runs PutRecordGetRecordExample against real Kinesis and Glue resources."
    mainClass.set("com.amazonaws.services.schemaregistry.examples.kds.PutRecordGetRecordExample")
    classpath = sourceSets["main"].runtimeClasspath
}
