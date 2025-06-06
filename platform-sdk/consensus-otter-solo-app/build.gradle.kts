// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "Otter Solo App"

mainModuleInfo {
    // This is needed to pick up and include the native libraries for the netty epoll transport
    runtimeOnly("io.netty.transport.epoll.linux.x86_64")
    runtimeOnly("io.netty.transport.epoll.linux.aarch_64")
    runtimeOnly("io.helidon.grpc.core")
    runtimeOnly("io.helidon.webclient")
    runtimeOnly("io.helidon.webclient.grpc")
}

// Add all the libs dependencies into the jar manifest!
tasks.jar {
    inputs.files(configurations.runtimeClasspath)
    manifest { attributes("Main-Class" to "org.hiero.consensus.otter.solo.SoloApp") }
    doFirst {
        manifest.attributes(
            "Class-Path" to
                inputs.files
                    .filter { it.extension == "jar" }
                    .map { "../../data/lib/" + it.name }
                    .sorted()
                    .joinToString(separator = " ")
        )
    }
}

// Copy dependencies into `data/lib`
val copyLib =
    tasks.register<Sync>("copyLib") {
        from(project.configurations.getByName("runtimeClasspath"))
        into(layout.projectDirectory.dir("build/data/lib"))
    }

// Copy built jar into `data/apps` and rename HederaNode.jar
val copyApp =
    tasks.register<Sync>("copyApp") {
        from(tasks.jar)
        into(layout.projectDirectory.dir("build/data/apps"))
        rename { "HederaNode.jar" }
        shouldRunAfter(tasks.named("copyLib"))
    }

tasks.assemble {
    dependsOn(copyLib)
    dependsOn(copyApp)
}
