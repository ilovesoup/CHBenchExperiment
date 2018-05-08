#pragma once

#include <cstdint>

typedef std::int8_t   Int8;
typedef std::uint8_t  UInt8;
typedef std::int16_t  Int16;
typedef std::uint16_t UInt16;
typedef std::int32_t  Int32;
typedef std::uint32_t UInt32;
typedef std::int64_t  Int64;
typedef std::uint64_t UInt64;

typedef UInt8 Filter;

#define ALWAYS_INLINE inline __attribute__((always_inline))
#define NO_INLINE __attribute__((__noinline__))

using AggregateDataPtr = char *;

#define unlikely(x)    (__builtin_expect(!!(x), 0))


template <typename T> struct NearestFieldType;

template <> struct NearestFieldType<UInt8>   { using Type = UInt64; };
template <> struct NearestFieldType<UInt16>  { using Type = UInt64; };
template <> struct NearestFieldType<UInt32>  { using Type = UInt64; };
template <> struct NearestFieldType<UInt64>  { using Type = UInt64; };
template <> struct NearestFieldType<Int8>    { using Type = Int64; };
template <> struct NearestFieldType<Int16>   { using Type = Int64; };
template <> struct NearestFieldType<Int32>   { using Type = Int64; };
template <> struct NearestFieldType<Int64>   { using Type = Int64; };
