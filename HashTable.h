#pragma once

#include <string.h>

#include <math.h>

#include <utility>

#include <boost/noncopyable.hpp>
#include <type_traits>
#include "types.h"

#include <Allocator.h>


using HashTableAllocator = Allocator<true>;

template <size_t N = 64>
using HashTableAllocatorWithStackMemory = AllocatorWithStackMemory<HashTableAllocator, N>;



#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
    #include <iostream>
    #include <iomanip>
    #include <Common/Stopwatch.h>
#endif

/** NOTE HashTable could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key in hash table must be of type, that zero bytes is compared equals to zero key.
  */




/// These functions can be overloaded for custom types.
namespace ZeroTraits
{

template <typename T>
bool check(const T x) { return x == 0; }

template <typename T>
void set(T & x) { x = 0; }

};


/** Compile-time interface for cell of the hash table.
  * Different cell types are used to implement different hash tables.
  * The cell must contain a key.
  * It can also contain a value and arbitrary additional data
  *  (example: the stored hash value; version number for ClearableHashMap).
  */
template <typename Key, typename Hash>
struct HashTableCell
{
    using value_type = Key;
    Key key;

    HashTableCell() {}

    /// Create a cell with the given key / key and value.
    HashTableCell(const Key & key_) : key(key_) {}
/// HashTableCell(const value_type & value_, const State & state) : key(value_) {}

    /// Get what the value_type of the container will be.
    value_type & getValue()             { return key; }
    const value_type & getValue() const { return key; }

    /// Get the key.
    static Key & getKey(value_type & value)             { return value; }
    static const Key & getKey(const value_type & value) { return value; }

    /// Are the keys at the cells equal?
    bool keyEquals(const Key & key_) const { return key == key_; }
    bool keyEquals(const Key & key_, size_t /*hash_*/) const { return key == key_; }

    /// If the cell can remember the value of the hash function, then remember it.
    void setHash(size_t /*hash_value*/) {}

    /// If the cell can store the hash value in itself, then return the stored value.
    /// It must be at least once calculated before.
    /// If storing the hash value is not provided, then just compute the hash.
    size_t getHash(const Hash & hash) const { return hash(key); }

    /// Whether the key is zero. In the main buffer, cells with a zero key are considered empty.
    /// If zero keys can be inserted into the table, then the cell for the zero key is stored separately, not in the main buffer.
    /// Zero keys must be such that the zeroed-down piece of memory is a zero key.
    bool isZero() const { return isZero(key); }
    static bool isZero(const Key & key) { return ZeroTraits::check(key); }

    /// Set the key value to zero.
    void setZero() { ZeroTraits::set(key); }

    /// Do the hash table need to store the zero key separately (that is, can a zero key be inserted into the hash table).
    static constexpr bool need_zero_value_storage = true;

    /// Whether the cell is deleted.
    bool isDeleted() const { return false; }

    /// Set the mapped value, if any (for HashMap), to the corresponding `value`.
    void setMapped(const value_type & /*value*/) {}
};


/** Determines the size of the hash table, and when and how much it should be resized.
  */
template <size_t initial_size_degree = 8>
struct HashTableGrower
{
    /// The state of this structure is enough to get the buffer size of the hash table.

    UInt8 size_degree = initial_size_degree;

    /// The size of the hash table in the cells.
    size_t bufSize() const               { return 1ULL << size_degree; }

    size_t maxFill() const               { return 1ULL << (size_degree - 1); }
    size_t mask() const                  { return bufSize() - 1; }

    /// From the hash value, get the cell number in the hash table.
    size_t place(size_t x) const         { return x & mask(); }

    /// The next cell in the collision resolution chain.
    size_t next(size_t pos) const        { ++pos; return pos & mask(); }

    /// Whether the hash table is sufficiently full. You need to increase the size of the hash table, or remove something unnecessary from it.
    bool overflow(size_t elems) const    { return elems > maxFill(); }

    /// Increase the size of the hash table.
    void increaseSize()
    {
        size_degree += size_degree >= 23 ? 1 : 2;
    }

    /// Set the buffer size by the number of elements in the hash table. Used when deserializing a hash table.
    void set(size_t num_elems)
    {
        size_degree = num_elems <= 1
             ? initial_size_degree
             : ((initial_size_degree > static_cast<size_t>(log2(num_elems - 1)) + 2)
                 ? initial_size_degree
                 : (static_cast<size_t>(log2(num_elems - 1)) + 2));
    }

    void setBufSize(size_t buf_size_)
    {
        size_degree = static_cast<size_t>(log2(buf_size_ - 1) + 1);
    }
};


/** When used as a Grower, it turns a hash table into something like a lookup table.
  * It remains non-optimal - the cells store the keys.
  * Also, the compiler can not completely remove the code of passing through the collision resolution chain, although it is not needed.
  * TODO Make a proper lookup table.
  */
template <size_t key_bits>
struct HashTableFixedGrower
{
    size_t bufSize() const               { return 1ULL << key_bits; }
    size_t place(size_t x) const         { return x; }
    /// You could write __builtin_unreachable(), but the compiler does not optimize everything, and it turns out less efficiently.
    size_t next(size_t pos) const        { return pos + 1; }
    bool overflow(size_t /*elems*/) const { return false; }

    void increaseSize() { __builtin_unreachable(); }
    void set(size_t /*num_elems*/) {}
    void setBufSize(size_t /*buf_size_*/) {}
};


/** If you want to store the zero key separately - a place to store it. */
template <bool need_zero_value_storage, typename Cell>
struct ZeroValueStorage;

template <typename Cell>
struct ZeroValueStorage<true, Cell>
{
private:
    bool has_zero = false;
    std::aligned_storage_t<sizeof(Cell), alignof(Cell)> zero_value_storage; /// Storage of element with zero key.

public:
    bool hasZero() const { return has_zero; }
    void setHasZero() { has_zero = true; }
    void clearHasZero() { has_zero = false; }

    Cell * zeroValue()              { return reinterpret_cast<Cell*>(&zero_value_storage); }
    const Cell * zeroValue() const  { return reinterpret_cast<const Cell*>(&zero_value_storage); }
};

template <typename Cell>
struct ZeroValueStorage<false, Cell>
{
    bool hasZero() const { return false; }
    void setHasZero() { throw "too bad"; }
    void clearHasZero() {}

    Cell * zeroValue()              { return nullptr; }
    const Cell * zeroValue() const  { return nullptr; }
};


template
<
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower,
    typename Allocator
>
class HashTable :
    private boost::noncopyable,
    protected Hash,
    protected Allocator,
    protected ZeroValueStorage<Cell::need_zero_value_storage, Cell>     /// empty base optimization
{
protected:
    friend class const_iterator;
    friend class iterator;

    template <typename, typename, typename, typename, typename, typename, size_t>

    using HashValue = size_t;
    using Self = HashTable<Key, Cell, Hash, Grower, Allocator>;
    using cell_type = Cell;

    size_t m_size = 0;        /// Amount of elements
    Cell * buf;               /// A piece of memory for all elements except the element with zero key.
    Grower grower;

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    mutable size_t collisions = 0;
#endif

    /// Find a cell with the same key or an empty cell, starting from the specified position and further along the collision resolution chain.
    size_t ALWAYS_INLINE findCell(const Key & x, size_t hash_value, size_t place_value) const
    {
        while (!buf[place_value].isZero() && !buf[place_value].keyEquals(x, hash_value))
        {
            place_value = grower.next(place_value);
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            ++collisions;
#endif
        }

        return place_value;
    }


    /// Find an empty cell, starting with the specified position and further along the collision resolution chain.
    size_t ALWAYS_INLINE findEmptyCell(size_t place_value) const
    {
        while (!buf[place_value].isZero())
        {
            place_value = grower.next(place_value);
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            ++collisions;
#endif
        }

        return place_value;
    }

    void alloc(const Grower & new_grower)
    {
        buf = reinterpret_cast<Cell *>(Allocator::alloc(new_grower.bufSize() * sizeof(Cell)));
        grower = new_grower;
    }

    void free()
    {
        if (buf)
        {
            Allocator::free(buf, getBufferSizeInBytes());
            buf = nullptr;
        }
    }


    /// Increase the size of the buffer.
    void resize(size_t for_num_elems = 0, size_t for_buf_size = 0)
    {
#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
        Stopwatch watch;
#endif

        size_t old_size = grower.bufSize();

        /** In case of exception for the object to remain in the correct state,
          *  changing the variable `grower` (which determines the buffer size of the hash table)
          *  is postponed for a moment after a real buffer change.
          * The temporary variable `new_grower` is used to determine the new size.
          */
        Grower new_grower = grower;

        if (for_num_elems)
        {
            new_grower.set(for_num_elems);
            if (new_grower.bufSize() <= old_size)
                return;
        }
        else if (for_buf_size)
        {
            new_grower.setBufSize(for_buf_size);
            if (new_grower.bufSize() <= old_size)
                return;
        }
        else
            new_grower.increaseSize();

        /// Expand the space.
        buf = reinterpret_cast<Cell *>(Allocator::realloc(buf, getBufferSizeInBytes(), new_grower.bufSize() * sizeof(Cell)));
        grower = new_grower;

        /** Now some items may need to be moved to a new location.
          * The element can stay in place, or move to a new location "on the right",
          *  or move to the left of the collision resolution chain, because the elements to the left of it have been moved to the new "right" location.
          */
        size_t i = 0;
        for (; i < old_size; ++i)
            if (!buf[i].isZero() && !buf[i].isDeleted())
                reinsert(buf[i], buf[i].getHash(*this));

        /** There is also a special case:
          *    if the element was to be at the end of the old buffer,                  [        x]
          *    but is at the beginning because of the collision resolution chain,      [o       x]
          *    then after resizing, it will first be out of place again,               [        xo        ]
          *    and in order to transfer it where necessary,
          *    after transferring all the elements from the old halves you need to     [         o   x    ]
          *    process tail from the collision resolution chain immediately after it   [        o    x    ]
          */
        for (; !buf[i].isZero() && !buf[i].isDeleted(); ++i)
            reinsert(buf[i], buf[i].getHash(*this));

#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
        watch.stop();
        std::cerr << std::fixed << std::setprecision(3)
            << "Resize from " << old_size << " to " << grower.bufSize() << " took " << watch.elapsedSeconds() << " sec."
            << std::endl;
#endif
    }


    /** Paste into the new buffer the value that was in the old buffer.
      * Used when increasing the buffer size.
      */
    void reinsert(Cell & x, size_t hash_value)
    {
        size_t place_value = grower.place(hash_value);

        /// If the element is in its place.
        if (&x == &buf[place_value])
            return;

        /// Compute a new location, taking into account the collision resolution chain.
        place_value = findCell(Cell::getKey(x.getValue()), hash_value, place_value);

        /// If the item remains in its place in the old collision resolution chain.
        if (!buf[place_value].isZero())
            return;

        /// Copy to a new location and zero the old one.
        x.setHash(hash_value);
        memcpy(&buf[place_value], &x, sizeof(x));
        x.setZero();

        /// Then the elements that previously were in collision with this can move to the old place.
    }


    void destroyElements()
    {
    		for (iterator it = begin(); it != end(); ++it)
    			it.ptr->~Cell();
    }


    template <typename Derived, bool is_const>
    class iterator_base
    {
        using Container = std::conditional_t<is_const, const Self, Self>;
        using cell_type = std::conditional_t<is_const, const Cell, Cell>;

        Container * container;
        cell_type * ptr;

        friend class HashTable;

    public:
        iterator_base() {}
        iterator_base(Container * container_, cell_type * ptr_) : container(container_), ptr(ptr_) {}

        bool operator== (const iterator_base & rhs) const { return ptr == rhs.ptr; }
        bool operator!= (const iterator_base & rhs) const { return ptr != rhs.ptr; }

        Derived & operator++()
        {
            if (__builtin_expect(ptr->isZero(), 0))
                ptr = container->buf;
            else
                ++ptr;

            while (ptr < container->buf + container->grower.bufSize() && ptr->isZero())
                ++ptr;

            return static_cast<Derived &>(*this);
        }

        auto & operator* () const { return ptr->getValue(); }
        auto * operator->() const { return &ptr->getValue(); }

        auto getPtr() const { return ptr; }
        size_t getHash() const { return ptr->getHash(*container); }

        size_t getCollisionChainLength() const
        {
            return container->grower.place((ptr - container->buf) - container->grower.place(getHash()));
        }
    };


public:
    using key_type = Key;
    using value_type = typename Cell::value_type;

    size_t hash(const Key & x) const { return Hash::operator()(x); }


    HashTable()
    {
        if (Cell::need_zero_value_storage)
            this->zeroValue()->setZero();
        alloc(grower);
    }

    HashTable(size_t reserve_for_num_elements)
    {
        if (Cell::need_zero_value_storage)
            this->zeroValue()->setZero();
        grower.set(reserve_for_num_elements);
        alloc(grower);
    }

    ~HashTable()
    {
        destroyElements();
        free();
    }

    class iterator : public iterator_base<iterator, false>
    {
    public:
        using iterator_base<iterator, false>::iterator_base;
    };

    class const_iterator : public iterator_base<const_iterator, true>
    {
    public:
        using iterator_base<const_iterator, true>::iterator_base;
    };


    const_iterator begin() const
    {
        if (!buf)
            return end();

        if (this->hasZero())
            return iteratorToZero();

        const Cell * ptr = buf;
        while (ptr < buf + grower.bufSize() && ptr->isZero())
            ++ptr;

        return const_iterator(this, ptr);
    }

    iterator begin()
    {
        if (!buf)
            return end();

        if (this->hasZero())
            return iteratorToZero();

        Cell * ptr = buf;
        while (ptr < buf + grower.bufSize() && ptr->isZero())
            ++ptr;

        return iterator(this, ptr);
    }

    const_iterator end() const         { return const_iterator(this, buf + grower.bufSize()); }
    iterator end()                     { return iterator(this, buf + grower.bufSize()); }


protected:
    const_iterator iteratorTo(const Cell * ptr) const { return const_iterator(this, ptr); }
    iterator iteratorTo(Cell * ptr)                   { return iterator(this, ptr); }
    const_iterator iteratorToZero() const             { return iteratorTo(this->zeroValue()); }
    iterator iteratorToZero()                         { return iteratorTo(this->zeroValue()); }


    /// If the key is zero, insert it into a special place and return true.
    bool ALWAYS_INLINE emplaceIfZero(Key x, iterator & it, bool & inserted, size_t hash_value)
    {
        /// If it is claimed that the zero key can not be inserted into the table.
        if (!Cell::need_zero_value_storage)
            return false;

        if (Cell::isZero(x))
        {
            it = iteratorToZero();
            if (!this->hasZero())
            {
                ++m_size;
                this->setHasZero();
                it.ptr->setHash(hash_value);
                inserted = true;
            }
            else
                inserted = false;

            return true;
        }

        return false;
    }


    /// Only for non-zero keys. Find the right place, insert the key there, if it does not already exist. Set iterator to the cell in output parameter.
    void ALWAYS_INLINE emplaceNonZero(Key x, iterator & it, bool & inserted, size_t hash_value)
    {
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));

        it = iterator(this, &buf[place_value]);

        if (!buf[place_value].isZero())
        {
            inserted = false;
            return;
        }

        new(&buf[place_value]) Cell(x);
        buf[place_value].setHash(hash_value);
        inserted = true;
        ++m_size;

        if (unlikely(grower.overflow(m_size)))
        {
            try
            {
                resize();
            }
            catch (...)
            {
                /** If we have not resized successfully, then there will be problems.
                  * There remains a key, but uninitialized mapped-value,
                  *  which, perhaps, can not even be called a destructor.
                  */
                --m_size;
                buf[place_value].setZero();
                throw;
            }

            it = find(x, hash_value);
        }
    }


public:
    /// Insert a value. In the case of any more complex values, it is better to use the `emplace` function.
    std::pair<iterator, bool> ALWAYS_INLINE insert(const value_type & x)
    {
        std::pair<iterator, bool> res;

        size_t hash_value = hash(Cell::getKey(x));
        if (!emplaceIfZero(Cell::getKey(x), res.first, res.second, hash_value))
            emplaceNonZero(Cell::getKey(x), res.first, res.second, hash_value);

        if (res.second)
            res.first.ptr->setMapped(x);

        return res;
    }


    /// Reinsert node pointed to by iterator
    void ALWAYS_INLINE reinsert(iterator & it, size_t hash_value)
    {
        reinsert(*it.getPtr(), hash_value);
    }


    /** Insert the key,
      * return an iterator to a position that can be used for `placement new` of value,
      * as well as the flag - whether a new key was inserted.
      *
      * You have to make `placement new` of value if you inserted a new key,
      * since when destroying a hash table, it will call the destructor!
      *
      * Example usage:
      *
      * Map::iterator it;
      * bool inserted;
      * map.emplace(key, it, inserted);
      * if (inserted)
      *     new(&it->second) Mapped(value);
      */
    void ALWAYS_INLINE emplace(Key x, iterator & it, bool & inserted)
    {
        size_t hash_value = hash(x);
        if (!emplaceIfZero(x, it, inserted, hash_value))
            emplaceNonZero(x, it, inserted, hash_value);
    }


    /// Same, but with a precalculated value of hash function.
    void ALWAYS_INLINE emplace(Key x, iterator & it, bool & inserted, size_t hash_value)
    {
        if (!emplaceIfZero(x, it, inserted, hash_value))
            emplaceNonZero(x, it, inserted, hash_value);
    }


    /// Copy the cell from another hash table. It is assumed that the cell is not zero, and also that there was no such key in the table yet.
    void ALWAYS_INLINE insertUniqueNonZero(const Cell * cell, size_t hash_value)
    {
        size_t place_value = findEmptyCell(grower.place(hash_value));

        memcpy(&buf[place_value], cell, sizeof(*cell));
        ++m_size;

        if (unlikely(grower.overflow(m_size)))
            resize();
    }


    iterator ALWAYS_INLINE find(Key x)
    {
        if (Cell::isZero(x))
            return this->hasZero() ? iteratorToZero() : end();

        size_t hash_value = hash(x);
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero() ? iterator(this, &buf[place_value]) : end();
    }


    const_iterator ALWAYS_INLINE find(Key x) const
    {
        if (Cell::isZero(x))
            return this->hasZero() ? iteratorToZero() : end();

        size_t hash_value = hash(x);
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero() ? const_iterator(this, &buf[place_value]) : end();
    }


    iterator ALWAYS_INLINE find(Key x, size_t hash_value)
    {
        if (Cell::isZero(x))
            return this->hasZero() ? iteratorToZero() : end();

        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero() ? iterator(this, &buf[place_value]) : end();
    }


    const_iterator ALWAYS_INLINE find(Key x, size_t hash_value) const
    {
        if (Cell::isZero(x))
            return this->hasZero() ? iteratorToZero() : end();

        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero() ? const_iterator(this, &buf[place_value]) : end();
    }


    bool ALWAYS_INLINE has(Key x) const
    {
        if (Cell::isZero(x))
            return this->hasZero();

        size_t hash_value = hash(x);
        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero();
    }


    bool ALWAYS_INLINE has(Key x, size_t hash_value) const
    {
        if (Cell::isZero(x))
            return this->hasZero();

        size_t place_value = findCell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].isZero();
    }

    size_t size() const
    {
        return m_size;
    }

    bool empty() const
    {
        return 0 == m_size;
    }

    void clear()
    {
        destroyElements();
        this->clearHasZero();
        m_size = 0;

        memset(buf, 0, grower.bufSize() * sizeof(*buf));
    }

    /// After executing this function, the table can only be destroyed,
    ///  and also you can use the methods `size`, `empty`, `begin`, `end`.
    void clearAndShrink()
    {
        destroyElements();
        this->clearHasZero();
        m_size = 0;
        free();
    }

    size_t getBufferSizeInBytes() const
    {
        return grower.bufSize() * sizeof(Cell);
    }

    size_t getBufferSizeInCells() const
    {
        return grower.bufSize();
    }

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    size_t getCollisions() const
    {
        return collisions;
    }
#endif
};
