# Plugin API compatibility and packaging

`hdata-plugin-api` is the narrow binary boundary between HData and an isolated connector. Its
compatibility version is the `api_version` in `META-INF/hdata-plugin.properties`, currently **1**.
This number is independent from the HData application artifact version: a compatible HData release
may change its own version while continuing to load API version 1 plugins.

## Compatibility policy

- An API version is immutable after release. Adding an incompatible SPI method, changing a type that
  crosses the boundary, or changing a serialized configuration/row contract requires a new API
  version.
- The host rejects a plugin whose declared `api_version` is different from its supported API version
  before graph construction. A plugin must be rebuilt against the new API before it can be loaded.
- A compatible plugin release may change only its private implementation and private dependencies.
  It must not expose client-library types through `TransformProvider`, config, or Beam row values.
- Plugin authors publish the plugin JAR, private dependency JARs, `hdata-plugin.lock`, SHA-256 file,
  source revision, API version, and the HData/runner versions used for verification. A checksum is an
  integrity record, not a signature or a trust decision.

## Package a plugin

Place the descriptor JAR and every private dependency JAR in one input directory, then create the
exact directory supplied to `--pluginDirectories`:

```bash
tools/package-plugin.sh build/my-plugin-jars plugins/my-plugin
java -cp "$HDATA_CLASSPATH" me.jayer.hdata.core.HData \
  --pipeline=job.yaml --pluginDirectories=plugins/my-plugin
```

The package command requires exactly one descriptor JAR and writes two files:

- `hdata-plugin.sha256`, with a SHA-256 digest for every packaged JAR;
- `hdata-plugin.lock`, recording the plugin id, API version, provider class, descriptor JAR, and
  checksum file name.

Verify before deployment and after distribution:

```bash
(cd plugins/my-plugin && sha256sum -c hdata-plugin.sha256)
```

HData currently verifies the descriptor/API version when it loads a plugin; operators verify the
lock-file checksums before it is staged to workers. Publish a signature alongside the checksum file
when the release channel provides signing infrastructure.
