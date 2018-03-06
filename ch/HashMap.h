#pragma once

#include "HashTable.h"


/** NOTE HashMap could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key in hash table must be of type, that zero bytes is compared equals to zero key.
  */


struct NoInitTag {};

/// A pair that does not initialize the elements, if not needed.
template <typename First, typename Second>
struct PairNoInit
{
    First first;
    Second second;

    PairNoInit() {}

    template <typename First_>
    PairNoInit(First_ && first_, NoInitTag)
        : first(std::forward<First_>(first_)) {}

    template <typename First_, typename Second_>
    PairNoInit(First_ && first_, Second_ && second_)
        : first(std::forward<First_>(first_)), second(std::forward<Second_>(second_)) {}
};

struct TrivialHash
{
    template <typename T>
    size_t operator() (T key) const
    {
        return key;
    }
};

template <typename Key, typename TMapped, typename Hash>
struct HashMapCell
{
    using Mapped = TMapped;

    using value_type = PairNoInit<Key, Mapped>;
    value_type value;

    HashMapCell() {}
    HashMapCell(const Key & key_) : value(key_, NoInitTag()) {}
    HashMapCell(const value_type & value_) : value(value_) {}

    value_type & getValue() { return value; }
    const value_type & getValue() const { return value; }

    static Key & getKey(value_type & value) { return value.first; }
    static const Key & getKey(const value_type & value) { return value.first; }

    bool keyEquals(const Key & key_) const { return value.first == key_; }
    bool keyEquals(const Key & key_, size_t /*hash_*/) const { return value.first == key_; }

    void setHash(size_t /*hash_value*/) {}
    size_t getHash(const Hash & hash) const { return hash(value.first); }

    bool isZero() const { return isZero(value.first); }
    static bool isZero(const Key & key) { return ZeroTraits::check(key); }

    /// Set the key value to zero.
    void setZero() { ZeroTraits::set(value.first); }

    /// Do I need to store the zero key separately (that is, can a zero key be inserted into the hash table).
    static constexpr bool need_zero_value_storage = true;

    /// Whether the cell was deleted.
    bool isDeleted() const { return false; }

    void setMapped(const value_type & value_) { value.second = value_.second; }
};

template
<
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator
>
class HashMapTable : public HashTable<Key, Cell, Hash, Grower, Allocator>
{
public:
    using key_type = Key;
    using mapped_type = typename Cell::Mapped;
    using value_type = typename Cell::value_type;

    using HashTable<Key, Cell, Hash, Grower, Allocator>::HashTable;

    inline __attribute__((always_inline)) mapped_type & operator[](Key x)
    {
        typename HashMapTable::iterator it;
        bool inserted;
        this->emplace(x, it, inserted);

        /** It may seem that initialization is not necessary for POD-types (or __has_trivial_constructor),
          *  since the hash table memory is initially initialized with zeros.
          * But, in fact, an empty cell may not be initialized with zeros in the following cases:
          * - ZeroValueStorage (it only zeros the key);
          * - after resizing and moving a part of the cells to the new half of the hash table, the old cells also have only the key to zero.
          *
          * On performance, there is almost always no difference, due to the fact that it->second is usually assigned immediately
          *  after calling `operator[]`, and since `operator[]` is inlined, the compiler removes unnecessary initialization.
          *
          * Sometimes due to initialization, the performance even grows. This occurs in code like `++map[key]`.
          * When we do the initialization, for new cells, it's enough to make `store 1` right away.
          * And if we did not initialize, then even though there was zero in the cell,
          *  the compiler can not guess about this, and generates the `load`, `increment`, `store` code.
          */
        if (inserted)
            new(&it->second) mapped_type();

        return it->second;
    }
};


template
<
    typename Key,
    typename Mapped,
    typename Hash,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator
>
using HashMap = HashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>;
