// Le build racine ne porte aucun code : la configuration commune vit dans les
// plugins de convention `gsr.*` (buildSrc), appliqués module par module.
tasks.register("printModules") {
    group = "help"
    description = "Liste les modules publiés et leur artifactId."
    val modules = subprojects.associate { it.path to it.name }
    doLast { modules.forEach { (path, name) -> println("$path -> $name") } }
}
