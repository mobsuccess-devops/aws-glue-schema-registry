plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

dependencies {
    api(project(":schema-registry-serde"))
    api(platform(libs.jackson.bom))
    api(libs.aws.v1.kinesis)
    api(libs.jackson.cbor)
    api(libs.commons.cli)
}
