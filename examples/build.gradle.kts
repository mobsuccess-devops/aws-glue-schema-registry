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
