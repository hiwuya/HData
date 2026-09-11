package me.jayer.hdata.hive.format

import java.util.Random

/**
 * Derives a stable, mutually decorrelated random seed for a file position.
 *
 * An SDF restriction must not simply call `Random(userSeed)`: when a restriction holds a single storage block, every block takes
 * the first value of the random sequence and they end up either all kept or all dropped. Mixing the file and block offset into
 * the seed makes the decision independent of how the restriction is split, so dynamic splitting or retries never change the
 */
internal fun samplingSeed(seed: Long, filePath: String, offset: Long): Long {
    var mixed = seed xor offset.rotateLeft(21) xor filePath.hashCode().toLong().rotateLeft(42)
    // SplitMix64's avalanche; this only mixes deterministically, it keeps no global random state.
    mixed = (mixed xor (mixed ushr 30)) * -4658895280553007687L
    mixed = (mixed xor (mixed ushr 27)) * -7723592293110705685L
    return mixed xor (mixed ushr 31)
}

internal fun sampleBlock(seed: Long, filePath: String, offset: Long): Double =
    Random(samplingSeed(seed, filePath, offset)).nextDouble()
