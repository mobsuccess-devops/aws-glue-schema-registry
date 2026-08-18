plugins {
    id("gsr.shaded-conventions")
}

dependencies {
    api(project(":schema-registry-serde"))
    api(libs.aws.mskIamAuth)
}

