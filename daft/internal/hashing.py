import numba
import numpy as np

# Implements Murmur hashing
# See: https://en.wikipedia.org/wiki/MurmurHash

# @numba.vectorize([numba.uint32(int_type) for int_type in numba.types.integer_domain], nopython=True)
@numba.njit(numba.uint32(numba.uint32), locals={"h": numba.uint32})
def murmur3_hash_32(h):
    h ^= h >> 16
    h *= 0x85EBCA6B
    h ^= h >> 13
    h *= 0xC2B2AE35
    h ^= h >> 16
    return h


# @numba.vectorize([numba.uint64(int_type) for int_type in numba.types.integer_domain], nopython=True)
def murmur3_hash_64(h):
    h ^= h >> 33
    h *= 0xFF51AFD7ED558CCD
    h ^= h >> 33
    h *= 0xC4CEB9FE1A85EC53
    h ^= h >> 33
    return h


@numba.njit(numba.uint32(numba.uint32, numba.int8), inline="always")
def rotl32(x, r):
    return (x << r) | (x >> (32 - r))


@numba.njit(numba.uint32(numba.uint8[:]), locals={"c1": numba.uint32, "c1": numba.uint32, "k1": numba.uint32, "h1": numba.uint32})  # type: ignore
def murmur3_hash_32_buffer(buffer: np.ndarray) -> int:
    c1 = 0xCC9E2D51
    c2 = 0x1B873593

    h1 = 0
    size = buffer.size
    nblocks = size // 4
    residual = size % 4
    aligned_array = buffer[0 : nblocks * 4].view(np.uint32)
    for i in range(nblocks):
        k1 = aligned_array[i]

        k1 *= c1
        k1 = rotl32(k1, 15)
        k1 *= c2

        h1 ^= k1
        h1 = rotl32(h1, 13)
        h1 = h1 * 5 + 0xE6546B64

    k1 = 0
    offset = nblocks * 4
    for i in range(residual):
        idx = offset + i
        k1 = k1 ^ (buffer[idx] << (8 * (i)))

    k1 *= c1
    k1 = rotl32(k1, 15)

    k1 *= c2

    h1 ^= k1
    h1 ^= size
    h1 = murmur3_hash_32(h1)
    return h1
