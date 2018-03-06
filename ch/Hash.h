#pragma once

#include "types.h"

#if __SSE4_2__
#include <nmmintrin.h>
#endif



inline UInt64 intHash64(UInt64 x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;

    return x;
}

inline UInt64 intHashCRC32(UInt64 x)
{
#if __SSE4_2__
    return _mm_crc32_u64(-1ULL, x);
#else
    /// On other platforms we do not have CRC32. NOTE This can be confusing.
    return intHash64(x);
#endif
}


template <typename T> struct HashCRC32;

template <typename T>
inline size_t hashCRC32(T key)
{
    union
    {
        T in;
        UInt64 out;
    } u;
    u.out = 0;
    u.in = key;
    return intHashCRC32(u.out);
}

template <typename T> struct HashCRC32
{
    size_t operator() (T key) const\
    {
        return hashCRC32<T>(key);\
    }
};
