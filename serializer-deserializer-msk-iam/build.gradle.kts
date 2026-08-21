plugins {
    id("gsr.shaded-conventions")
}

dependencies {
    implementation(project(":schema-registry-serde"))
    implementation(libs.aws.mskIamAuth)
}
