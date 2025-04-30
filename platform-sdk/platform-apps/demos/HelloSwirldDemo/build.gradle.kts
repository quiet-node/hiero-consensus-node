// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.application")
    id("org.openjfx.javafxplugin") version "0.1.0"
}
javafx {
    modules("javafx.controls")
}
application.mainClass = "com.swirlds.demo.hello.HelloSwirldDemoMain"
