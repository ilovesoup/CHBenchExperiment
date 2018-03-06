#include <cstring>
#include "ColumnVector.h"
#include <iostream>

#if __SSE2__
    #include <emmintrin.h>
#endif


template <typename T>
shared_ptr<ColumnVector<T>> ColumnVector<T>::filter(Filter * filter, size_t result_size_hint) const {
    auto res = new ColumnVector(result_size_hint);
    T * res_data = res->pData.get();

    const UInt8 * filt_pos = filter;
    const UInt8 * filt_end = filt_pos + size;
    const T * data_pos = pData.get();
#if __SSE2__
    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */

    static constexpr size_t SIMD_BYTES = 16;
    static constexpr size_t SIMD_BYTES_IN_T = SIMD_BYTES * sizeof(T);
    const __m128i zero16 = _mm_setzero_si128();
    const UInt8 * filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;
    UInt64 sseHit = 0;
    UInt64 sseMiss = 0;

    while (filt_pos < filt_end_sse)
    {
        int mask = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));

        if (0 == mask)
        {
            /// Nothing is inserted.
        		sseHit ++;
        }
        else if (0xFFFF == mask)
        {
            memcpy(res_data, data_pos, SIMD_BYTES_IN_T);
            res_data += SIMD_BYTES;
            sseHit ++;
            // res_data.insert(data_pos, data_pos + SIMD_BYTES);
        }
        else
        {
        		sseMiss ++;
            for (size_t i = 0; i < SIMD_BYTES; ++i)
                if (filt_pos[i]) {
                    *res_data = data_pos[i];
                    res_data += 1;
                    // res_data.push_back(data_pos[i]);
                }
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }
#endif
    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            *res_data = *data_pos;
            //res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    std::cout << "SSE hits " << sseHit << std::endl;
    std::cout << "SSE miss " << sseMiss << std::endl;
    return shared_ptr<ColumnVector<T>>(res);
}

template class ColumnVector<Int32>;
template class ColumnVector<UInt32>;
