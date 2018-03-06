#pragma once

#include "HashMap.h"
#include "HashTable.h"
#include "Hash.h"
#include "types.h"
#include "Arena.h"
#include "ColumnVector.h"

template <typename T>
struct AggregateFunctionInstruction;

template <typename T>
struct AggregateFunctionSumData;

template <typename T>
struct AggregateFunctionSum;

/// For the case where there is one numeric key.
// For simplicity, using same type for both group by and aggregate type
template <typename FieldType, typename TData>    /// UInt8/16/32/64 for any type with corresponding bit width.
struct AggregationMethodOneNumber
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using iterator = typename Data::iterator;
    using const_iterator = typename Data::const_iterator;
    using AggType = FieldType;
    using AggregateInstructionType = AggregateFunctionInstruction<FieldType>;

    Data data;

    AggregationMethodOneNumber(ColumnVector<FieldType> * arguments) {
    		inst = new AggregateInstructionType[2];
    		inst->func = AggregateFunctionSum<FieldType>::addFree;
    		inst->state_offset = 0;
    		inst->arguments = arguments;
    		state.vec = arguments->getData();
    }

    /// To use one `Method` in different threads, use different `State`.
    struct State
    {
        FieldType * vec;

        /// Get the key from the key columns for insertion into the hash table.
        Key getKey(size_t i) const
        {
            return unionCastToUInt64(vec[i]);
        }
    };

    State state;

    /// From the value in the hash table, get AggregateDataPtr.
    static AggregateDataPtr & getAggregateData(Mapped & value)                { return value; }
    static const AggregateDataPtr & getAggregateData(const Mapped & value)    { return value; }

    /** Do not use optimization for consecutive keys.
      */
    static const bool no_consecutive_keys_optimization = false;

    AggregateInstructionType * inst = nullptr;
};

using AggregatedDataWithUInt64Key = HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>, HashTableGrower<>, HashTableAllocator>;
using AggregatedDataWithUInt16Key = HashMap<UInt64, AggregateDataPtr, TrivialHash, HashTableFixedGrower<16>, HashTableAllocator>;

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

    void add(AggregateDataPtr place, ColumnVector<T> * columns, size_t row_num, Arena *)
    {
        this->data(place).add(static_cast<ColumnVector<T> &>(*columns).getData()[row_num]);
    }

    static void addFree(AggregateFunctionSum<T> * that, AggregateDataPtr place, ColumnVector<T> * columns, size_t row_num, Arena * arena)
    {
        static_cast<AggregateFunctionSum<T> &>(*that).add(place, columns, row_num, arena);
    }

    void create(AggregateDataPtr place) const
	{
		new (place) Data;
	}

    AggregateFunctionSum<T> * that;
};

// Actual type value binding is not here but for simplicity we put it here
template <typename T>
struct AggregateFunctionInstruction
{
	void (* func)(AggregateFunctionSum<T> * that, AggregateDataPtr place, ColumnVector<T> * columns, size_t row_num, Arena * arena);
    AggregateFunctionSum<T> * that = nullptr;
    size_t state_offset;
    ColumnVector<T> * arguments;
};

template <typename T, typename Method>
struct Aggretator {
	AggregateFunctionSum<T> * agg;
	void createAggregateStates(AggregateDataPtr & aggregate_data)
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
	    size_t rows,
		AggregateFunctionInstruction<T> * aggregate_instructions)
	{
		std::shared_ptr<Arena> aggregates_pool_ptr(new Arena());
		Arena * aggregates_pool = aggregates_pool_ptr.get();
	    /// NOTE When editing this code, also pay attention to SpecializedAggregator.h.

	    /// For all rows.
		typename Method::State & state = method.state;
	    typename Method::iterator it;
	    typename Method::Key prev_key;
	    for (size_t i = 0; i < rows; ++i)
	    {
	        bool inserted;          /// Inserted a new key, or was this key already?

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

	            AggregateDataPtr place = aggregates_pool->alloc(sizeof(typename Method::AggType));
	            createAggregateStates(place);
	            aggregate_data = place;
	        }

	        AggregateDataPtr value = Method::getAggregateData(it->second);

	        /// Add values to the aggregate functions.
	        for (AggregateFunctionInstruction<T> * inst = aggregate_instructions; inst->that; ++inst)
	            (*inst->func)(inst->that, value + inst->state_offset, inst->arguments, i, aggregates_pool);
	    }
	}
};
