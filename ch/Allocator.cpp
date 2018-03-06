#include <Allocator.h>

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif

#include <cstdlib>
#include <sys/mman.h>

#include <mremap.h>


/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif


/** Many modern allocators (for example, tcmalloc) do not do a mremap for realloc,
  *  even in case of large enough chunks of memory.
  * Although this allows you to increase performance and reduce memory consumption during realloc.
  * To fix this, we do mremap manually if the chunk of memory is large enough.
  * The threshold (64 MB) is chosen quite large, since changing the address space is
  *  very slow, especially in the case of a large number of threads.
  * We expect that the set of operations mmap/something to do/mremap can only be performed about 1000 times per second.
  *
  * PS. This is also required, because tcmalloc can not allocate a chunk of memory greater than 16 GB.
  */
static constexpr size_t MMAP_THRESHOLD = 64 * (1ULL << 20);
static constexpr size_t MMAP_MIN_ALIGNMENT = 4096;
static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;


template <bool clear_memory_>
void * Allocator<clear_memory_>::alloc(size_t size, size_t alignment)
{
    void * buf;

    if (size >= MMAP_THRESHOLD)
    {
        if (alignment > MMAP_MIN_ALIGNMENT)
            throw "Too Bad";

        buf = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (MAP_FAILED == buf)
            throw "Too Bad";

        /// No need for zero-fill, because mmap guarantees it.
    }
    else
    {
        if (alignment <= MALLOC_MIN_ALIGNMENT)
        {
            if (clear_memory)
                buf = ::calloc(size, 1);
            else
                buf = ::malloc(size);

            if (nullptr == buf)
            	throw "Too Bad";
        }
        else
        {
            buf = nullptr;
            int res = posix_memalign(&buf, alignment, size);

            if (0 != res)
            	throw "Too Bad";

            if (clear_memory)
                memset(buf, 0, size);
        }
    }

    return buf;
}


template <bool clear_memory_>
void Allocator<clear_memory_>::free(void * buf, size_t size)
{
    if (size >= MMAP_THRESHOLD)
    {
        if (0 != munmap(buf, size))
        	throw "Too Bad";
    }
    else
    {
        ::free(buf);
    }
}


template <bool clear_memory_>
void * Allocator<clear_memory_>::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
{
    if (old_size == new_size)
    {
        /// nothing to do.
    }
    else if (old_size < MMAP_THRESHOLD && new_size < MMAP_THRESHOLD && alignment <= MALLOC_MIN_ALIGNMENT)
    {
        buf = ::realloc(buf, new_size);

        if (nullptr == buf)
        	throw "Too Bad";

        if (clear_memory && new_size > old_size)
            memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
    }
    else if (old_size >= MMAP_THRESHOLD && new_size >= MMAP_THRESHOLD)
    {
        // On apple and freebsd self-implemented mremap used (common/mremap.h)
        buf = clickhouse_mremap(buf, old_size, new_size, MREMAP_MAYMOVE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (MAP_FAILED == buf)
        		throw "Too Bad";

        /// No need for zero-fill, because mmap guarantees it.
    }
    else if (old_size >= MMAP_THRESHOLD && new_size < MMAP_THRESHOLD)
    {
        void * new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, new_size);
        if (0 != munmap(buf, old_size))
        {
            ::free(new_buf);
            throw "Too Bad";
        }
        buf = new_buf;
    }
    else
    {
        void * new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, old_size);
        free(buf, old_size);
        buf = new_buf;
    }

    return buf;
}


/// Explicit template instantiations.
template class Allocator<true>;
template class Allocator<false>;
