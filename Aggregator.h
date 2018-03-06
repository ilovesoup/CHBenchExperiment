#pragma once

#include "HashMap.h"
#include "HashTable.h"
#include "Hash.h"
#include "types.h"
#include "Arena.h"
#include "ColumnVector.h"

/// For the case where there is one numeric key.
template <typename FieldType, typename TData>    /// UInt8/16/32/64 for any type with corresponding bit width.
struct AggregationMethodOneNumber
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using iterator = typename Data::iterator;
    using const_iterator = typename Data::const_iterator;
    using AggType = typename NearestFieldType<FieldType>::Type;

    Data data;

    AggregationMethodOneNumber() {}

    template <typename Other>
    AggregationMethodOneNumber(const Other & other) : data(other.data) {}

    /// To use one `Method` in different threads, use different `State`.
    struct State
    {
        const FieldType * vec;

        /// Get the key from the key columns for insertion into the hash table.
        Key getKey(size_t i) const
        {
            return unionCastToUInt64(vec[i]);
        }
    };

    /// From the value in the hash table, get AggregateDataPtr.
    static AggregateDataPtr & getAggregateData(Mapped & value)                { return value; }
    static const AggregateDataPtr & getAggregateData(const Mapped & value)    { return value; }

    /** Do not use optimization for consecutive keys.
      */
    static const bool no_consecutive_keys_optimization = false;
};

using AggregatedDataWithUInt64Key = HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>, HashTableFixedGrower<16>, HashTableAllocator>;
using AggregatedDataWithUInt16Key = HashMap<UInt64, AggregateDataPtr, TrivialHash, HashTableFixedGrower<16>, HashTableAllocator>;
using Method = AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64Key>;

template <typename T>
struct AggregateFunctionSumData {
    T sum{};

    void add(T value)
    {
        sum += value;
    }

    T get() const
    {
        return sum;
    }
};

template <typename T>
struct AggregateFunctionSum {
	using Data = AggregateFunctionSumData<T>;
	static Data & data(AggregateDataPtr place) { return *reinterpret_cast<Data*>(place); }

    void add(AggregateDataPtr place, const ColumnVector<T> ** columns, size_t row_num, Arena *) const
    {
        this->data(place).add(static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    static void addFree(const AggregateFunctionSum<T> * that, AggregateDataPtr place, const ColumnVector<T> ** columns, size_t row_num, Arena * arena)
    {
        static_cast<const AggregateFunctionSum<T> &>(*that).add(place, columns, row_num, arena);
    }

    void create(AggregateDataPtr place) const
	{
		new (place) Data;
	}

    AggregateFunctionSum<T> * that;
};

template <typename T>
struct AggregateFunctionInstruction
{
	void (* func)(const AggregateFunctionSum<T> * that, AggregateDataPtr place, const ColumnVector<T> ** columns, size_t row_num, Arena * arena);
    const AggregateFunctionSum<T> * that;
    size_t state_offset;
    const ColumnVector<T> ** arguments;
};

template <typename T>
struct Aggretator {
	AggregateFunctionSum<T> * agg;
	void createAggregateStates(AggregateDataPtr & aggregate_data) const
	{
		try
		{
			agg->create(aggregate_data);
		}
		catch (...)
		{
			throw;
		}
	}

	void executeImplCase(
	    Method & method,
	    typename Method::State & state,
	    size_t rows,
		AggregateFunctionInstruction<T> * aggregate_instructions,
	    AggregateDataPtr overflow_row) const
	{
		std::shared_ptr<Arena> aggregates_pool_ptr(new Arena());
		Arena * aggregates_pool = aggregates_pool_ptr.get();
	    /// NOTE When editing this code, also pay attention to SpecializedAggregator.h.

	    /// For all rows.
	    typename Method::iterator it;
	    typename Method::Key prev_key;
	    for (size_t i = 0; i < rows; ++i)
	    {
	        bool inserted;          /// Inserted a new key, or was this key already?
	        bool overflow = false;  /// The new key did not fit in the hash table because of no_more_keys.

	        /// Get the key to insert into the hash table.
	        typename Method::Key key = state.getKey(i);

	        if (i != 0 && key == prev_key)
	        {
	            /// Add values to the aggregate functions.
	            AggregateDataPtr value = Method::getAggregateData(it->second);
	            for (AggregateFunctionInstruction<T> * inst = aggregate_instructions; inst->that; ++inst)
	                (*inst->func)(inst->that, value + inst->state_offset, inst->arguments, i, aggregates_pool);

	            continue;
	        }
	        else
	            prev_key = key;

	        method.data.emplace(key, it, inserted);

	        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
	        if (inserted)
	        {
	            AggregateDataPtr & aggregate_data = Method::getAggregateData(it->second);

	            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
	            aggregate_data = nullptr;

	            AggregateDataPtr place = aggregates_pool->alloc(sizeof(Method::AggType));
	            createAggregateStates(place);
	            aggregate_data = place;
	        }

	        AggregateDataPtr value = (!overflow) ? Method::getAggregateData(it->second) : overflow_row;

	        /// Add values to the aggregate functions.
	        for (AggregateFunctionInstruction<T> * inst = aggregate_instructions; inst->that; ++inst)
	            (*inst->func)(inst->that, value + inst->state_offset, inst->arguments, i, aggregates_pool);
	    }
	}
};
