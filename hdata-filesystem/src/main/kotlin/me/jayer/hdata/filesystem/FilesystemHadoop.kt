package me.jayer.hdata.filesystem

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions
import org.apache.hadoop.conf.Configuration

/** Configures Beam's Hadoop filesystem implementation for the current pipeline. */
internal object FilesystemHadoop {

    fun configure(pipeline: Pipeline, defaultFs: String, values: Map<String, String>) {
        val configuration = Configuration(false).apply {
            set("fs.defaultFS", defaultFs)
            values.forEach { (key, value) -> set(key, value) }
        }
        pipeline.options.`as`(HadoopFileSystemOptions::class.java)
            .hdfsConfiguration = listOf(configuration)
    }
}
