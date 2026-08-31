plugins {
    id("gsr.distribution-conventions")
}

dependencies {
    implementation(project(":schema-registry-serde"))
    implementation(libs.aws.mskIamAuth)
}
