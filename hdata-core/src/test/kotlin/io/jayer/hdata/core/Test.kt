package io.jayer.hdata.core

import org.snakeyaml.engine.v1.api.Load
import org.snakeyaml.engine.v1.api.LoadSettingsBuilder
import java.io.FileReader

fun main() {
    val load = Load(LoadSettingsBuilder().build())
    val map = load.loadFromReader(FileReader("/Users/jayer/IdeaProjects/hdata/job.yml")) as Map<String, Any>
    map.forEach { (k, v) -> println("$k, ${v.javaClass}") }
}