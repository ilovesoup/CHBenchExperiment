#pragma once

#include "types.h"
#include <memory>

using std::shared_ptr;

template <typename T>
inline UInt64 unionCastToUInt64(T x) { return x; }

template <typename T>
class ColumnVector {
public:
    ColumnVector(shared_ptr<T> pData, size_t size) {
        this->pData = pData;
        this->size = size;
    }

    ColumnVector(size_t size) {
        pData = shared_ptr<T>(new T[size]);
        this->size = size;
    }

    shared_ptr<ColumnVector<T>> filter(Filter * filter, size_t result_size_hint) const;
    //shared_ptr<ColumnVector<T>> permute() const;
    T * getData() { return pData.get(); }
    size_t getSize() { return size; }
private:
    shared_ptr<T> pData;
    size_t size;
};

