#include <iostream>
#include <cstring>
#include <cstdlib>
#include <algorithm>
#include "ColumnVector.h"
#include "benchmark.h"
#include <cstdio>
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>
#include "Aggregator.h"
#include "FunctionsArithmetic.h"

using namespace std;


using AggregateFunctionSumDataInt32 = AggregateFunctionSumData<Int32>;
using Method = AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64Key>;

void testExpr(shared_ptr<UInt32>, shared_ptr<UInt32>, size_t);
void testExprCodegenStyle(shared_ptr<UInt32>, shared_ptr<UInt32>, size_t);
void testNaiveLocality(UInt32 * pData, size_t len);
void benchmark();
void testFilter(shared_ptr<UInt32> pData, size_t len);
void testAggregates(shared_ptr<UInt32> keyPtr, shared_ptr<UInt32> dataPtr, size_t len);

void handler(int sig) {
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}


void benchmark() {
	constexpr size_t bytes_in_gb = 1024 * 1024 * 1024;
	constexpr size_t num_gb = 1;
	constexpr size_t length = bytes_in_gb * num_gb / sizeof(UInt32);
    cout << "Total size " << num_gb << "G" << endl;
    shared_ptr<UInt32> dataPtr = shared_ptr<UInt32>(new UInt32[length], std::default_delete<UInt32[]>());
    shared_ptr<UInt32> keyPtr = shared_ptr<UInt32>(new UInt32[length], std::default_delete<UInt32[]>());
    UInt32 * pData = dataPtr.get();
    UInt32 * pKey = keyPtr.get();
    for (size_t i = 0; i < length; i++) {
        //pData[i] = rand() % 1000;
    		pKey[i] = i % 50 + 1;
    		pData[i] = i % 50 + 1;
    }
    /*
    cout << "benchmark started." << endl;
    //testNaiveLocality(pData, length);
    cout << "Test random data" << endl;
    testFilter(dataPtr, length);

    cout << "sorting data." << endl;
    sort(pData, pData + length);
    cout << "Test sorted data" << endl;
    testFilter(dataPtr, length);
	
    cout << "benchmark aggregates started." << endl;
    testAggregates(keyPtr, dataPtr, length);
    */
    cout << "benchmark expr started." << endl;
    testExpr(dataPtr, keyPtr, length);
    testExprCodegenStyle(dataPtr, keyPtr, length);
}

void testAggregates(shared_ptr<UInt32> keyPtr, shared_ptr<UInt32> dataPtr, size_t len) {
	Timer t;
	UInt32 * pData = dataPtr.get();
	UInt32 * pKey = keyPtr.get();
	ColumnVector<UInt32> vData(dataPtr, len);
	ColumnVector<UInt32> vKey(keyPtr, len);

	Aggretator<UInt32, Method> agg;

	Method m(&vData, &vKey);
	agg.executeImplCase(m, len, m.inst);
	PRINT_MIL("Aggregates", t.stop());

	Method::Data & data = m.data;
    for (auto & cell : data)
    {
        UInt32 key = cell.first;
        UInt32 value = *((UInt32 *)cell.second);

        cout << "key=" << key << "\tvalue=" << value << endl;
    }
}

template <typename T>
struct Pred {
    inline UInt8 operator() (T data) {
        return data < 500 ? 1 : 0;
    }
};

void testExpr(shared_ptr<UInt32> dataPtr, shared_ptr<UInt32> data1Ptr, size_t len) {
    Timer t;
	UInt32 * pData = dataPtr.get();
    UInt32 * pData1 = data1Ptr.get();
    BinaryOperationImpl<UInt32, UInt32, PlusImpl<UInt32, UInt32>> plus;

    UInt32 * pRes = new UInt32[len];
    UInt32 * pRes1 = new UInt32[len];

    plus.vector_constant(pData, 1, pRes, len);
    plus.vector_vector(pData1, pRes, pRes1, len);
    plus.vector_vector(pData1, pRes1, pRes, len);
    
    PRINT_MIL("Expr", t.stop());
    for (int i = 0; i < len; i++) {
    	if (pRes[i] != pData[i] + 1 + pData1[i] + pData1[i]) {
            cout << "Incorrect" << endl;
            delete [] pRes;
            delete [] pRes1;
            return;
        }
    }

    delete [] pRes;
    delete [] pRes1;
}

void testExprCodegenStyle(shared_ptr<UInt32> dataPtr, shared_ptr<UInt32> data1Ptr, size_t len) {
    Timer t;
	UInt32 * pData = dataPtr.get();
    UInt32 * pData1 = data1Ptr.get();

    BinaryOperationImpl<UInt32, UInt32, PlusImpl<UInt32, UInt32>> plus;

    UInt32 * pRes = new UInt32[len];

    for (int i = 0; i < len; i++) {
        pRes[i] = pData[i] + 1 + pData1[i] + pData1[i];
    }
    
    PRINT_MIL("Expr", t.stop());
    for (int i = 0; i < len; i++) {
    	if (pRes[i] != pData[i] + 1 + pData[i] + pData[i]) {
            cout << "Incorrect" << endl;
            delete [] pRes;
            return;
        }
    }

    delete [] pRes;
}

void testFilter(shared_ptr<UInt32> dataPtr, size_t len) {
	Timer t;
	UInt32 * pData = dataPtr.get();
    ColumnVector<UInt32> v(dataPtr, len);
    Pred<UInt32> p;
    Filter * pFilter = new Filter[len];
    size_t hint = 0;
    for (int i = 0; i < len; i++) {
        UInt8 r = p(pData[i]);
        pFilter[i] = r;
        hint += r;
    }

    auto res = v.filter(pFilter, hint);
    UInt64 sum = 0;
    PRINT_MIL("Filtering", t.stop());
    for (int i = 0; i < len; i++) {
    		UInt32 v = pData[i];
    		if (p(v)) {
    			sum += v;
    		}
    }

    UInt32 * pRes = res->getData();
    size_t resLen = res->getSize();
    for (int i = 0; i < resLen; i++) {
		Int32 v = pRes[i];
		if (p(v)) {
			sum -= v;
		}
    }
    cout << (sum == 0 ? "Correct" : "Incorrect") << endl;
    delete [] pFilter;
}

void testNaiveLocality(UInt32 * pData, size_t len) {
    // Actually Intel CPU is able to do prefetch backwards.
    // Hence there is little difference for cache miss part
    // Without O3, forwarding iteration needs an extra mov
    // for variable len on frame stack therefore slower
    Timer t;
    unsigned long sum = 0;
    for (int i = 0; i < len; i++) {
        sum += pData[i];
    }
    // print sum preventing loop being optimized away
    cout << sum << endl;
    PRINT_MIL("Forwarding Summation", t.stop());

    sum = 0;
    t.restart();
    for (int i = len - 1; i >= 0; i--) {
        sum += pData[i];
    }
    cout << sum << endl;
    PRINT_MIL("Reverse Summation", t.stop());

    int * pIndex = new int[len];
    for (int i = 0; i < len; i++) {
        pIndex[i] = i;
    }

    sum = 0;
    t.restart();
    // Index access forward
    for (int i = 0; i < len; i++) {
        sum += pData[pIndex[i]];
    }
    cout << sum << endl;
    PRINT_MIL("Index based Sequential Summation", t.stop());

    // Random Index access
    random_shuffle(pIndex, pIndex + len);
    sum = 0;
    t.restart();
    for (int i = 0; i < len; i++) {
        sum += pData[pIndex[i]];
    }
    cout << sum << endl;
    PRINT_MIL("Index based Random Summation", t.stop());
}

int main() {
    int i;
    //cin >> i;
    signal(SIGSEGV, handler);
    benchmark();
    return 0;
}
