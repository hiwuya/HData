package me.jayer.hdata.filesystem

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions
import org.apache.hadoop.conf.Configuration

/** Configures Beam's Hadoop filesystem implementation for the current pipeline. */
internal object FilesystemHadoop {

    fun configure(pipeline: Pipeline, defaultFs: String, values: Map<String, String>) {
        // The plain local case needs no Hadoop bridge at all: Beam's built-in LocalFileSystem already handles file://
        // paths, and registering a HadoopFileSystem for "file" on top of it makes FileSystems.setDefaultPipelineOptions
        // reject the pipeline outright ("Scheme: [file] has conflicting filesystems") the moment it runs. Only bypass
        // this skip when the caller actually passed hadoop_conf overrides, e.g. to exercise Hadoop-specific behavior.
        if (defaultFs.startsWith("file:", ignoreCase = true) && values.isEmpty()) {
            return
        }
        val configuration = Configuration(false).apply {
            set("fs.defaultFS", defaultFs)
            set("hadoop.tmp.dir", System.getProperty("java.io.tmpdir"))
            if (defaultFs.startsWith("s3a://")) {
                set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            }
            values.forEach { (key, value) -> set(key, value) }
        }
        pipeline.options.`as`(HadoopFileSystemOptions::class.java)
            .hdfsConfiguration = listOf(configuration)
    }
}
