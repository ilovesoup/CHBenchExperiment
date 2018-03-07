package org.apache.spark.sql;

import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BenchmarkTest;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;

public class AggregateStandardNoNullableIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
  private boolean agg_initAgg;
  private agg_FastHashMap agg_fastHashMap;
  private org.apache.spark.unsafe.KVIterator agg_fastHashMapIter;
  private org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap agg_hashMap;
  private org.apache.spark.sql.execution.UnsafeKVExternalSorter agg_sorter;
  private org.apache.spark.unsafe.KVIterator agg_mapIter;
  private Iterator inputadapter_input;
  private UnsafeRow agg_result1;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder agg_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter;
  private int hashVal;

  public void init(Iterator input) {
    agg_initAgg = false;

    agg_fastHashMap = new agg_FastHashMap(BenchmarkTest.getTaskMemoryManager(), BenchmarkTest.getEmptyAggregationBuffer());

    inputadapter_input = input;
    agg_result1 = new UnsafeRow(1);
    this.agg_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(agg_result1, 0);
    this.agg_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(agg_holder, 1);
  }

  public class agg_FastHashMap {
    private org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch batch;
    private int[] buckets;
    private int capacity = 1 << 16;
    private double loadFactor = 0.5;
    private int numBuckets = (int) (capacity / loadFactor);
    private int maxSteps = 2;
    private int numRows = 0;
    private org.apache.spark.sql.types.StructType keySchema = new org.apache.spark.sql.types.StructType().add("((java.lang.String) references[3])", org.apache.spark.sql.types.DataTypes.IntegerType);
    private org.apache.spark.sql.types.StructType valueSchema = new org.apache.spark.sql.types.StructType().add("((java.lang.String) references[4])", org.apache.spark.sql.types.DataTypes.LongType);
    private Object emptyVBase;
    private long emptyVOff;
    private int emptyVLen;
    private boolean isBatchFull = false;

    public agg_FastHashMap(
        org.apache.spark.memory.TaskMemoryManager taskMemoryManager,
        InternalRow emptyAggregationBuffer) {
      batch = org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch
          .allocate(keySchema, valueSchema, taskMemoryManager, capacity);

      final UnsafeProjection valueProjection = UnsafeProjection.create(valueSchema);
      final byte[] emptyBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();

      emptyVBase = emptyBuffer;
      emptyVOff = Platform.BYTE_ARRAY_OFFSET;
      emptyVLen = emptyBuffer.length;

      buckets = new int[numBuckets];
      java.util.Arrays.fill(buckets, -1);
    }

    public org.apache.spark.sql.catalyst.expressions.UnsafeRow findOrInsert(int agg_key) {
      long h = hash(agg_key);
      int step = 0;
      int idx = (int) h & (numBuckets - 1);
      while (step < maxSteps) {
        // Return bucket index if it's either an empty slot or already contains the key
        if (buckets[idx] == -1) {
          if (numRows < capacity && !isBatchFull) {
            // creating the unsafe for new entry
            UnsafeRow agg_result = new UnsafeRow(1);
            org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder agg_holder
                = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(agg_result,
                0);
            org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter
                = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(
                agg_holder,
                1);
            agg_holder.reset(); //TODO: investigate if reset or zeroout are actually needed
            agg_rowWriter.zeroOutNullBytes();
            agg_rowWriter.write(0, agg_key);
            agg_result.setTotalSize(agg_holder.totalSize());
            Object kbase = agg_result.getBaseObject();
            long koff = agg_result.getBaseOffset();
            int klen = agg_result.getSizeInBytes();

            UnsafeRow vRow
                = batch.appendRow(kbase, koff, klen, emptyVBase, emptyVOff, emptyVLen);
            if (vRow == null) {
              isBatchFull = true;
            } else {
              buckets[idx] = numRows++;
            }
            return vRow;
          } else {
            // No more space
            return null;
          }
        } else if (equals(idx, agg_key)) {
          return batch.getValueRow(buckets[idx]);
        }
        idx = (idx + 1) & (numBuckets - 1);
        step++;
      }
      // Didn't find it
      return null;
    }

    private boolean equals(int idx, int agg_key) {
      UnsafeRow row = batch.getKeyRow(buckets[idx]);
      return (row.getInt(0) == agg_key);
    }

    private long hash(int agg_key) {
      long agg_hash = 0;

      int agg_result = agg_key;
      agg_hash = (agg_hash ^ (0x9e3779b9)) + agg_result + (agg_hash << 6) + (agg_hash >>> 2);

      return agg_hash;
    }

    public org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator() {
      return batch.rowIterator();
    }

    public void close() {
      batch.close();
    }

  }

  private void agg_doAggregateWithKeys() throws java.io.IOException {
    agg_hashMap = BenchmarkTest.createHashMap();

    while (inputadapter_input.hasNext()) {
      InternalRow inputadapter_row = (InternalRow) inputadapter_input.next();
      int groupKey = inputadapter_row.getInt(0);
      int aggParam = inputadapter_row.getInt(1);

      UnsafeRow agg_unsafeRowAggBuffer = null;

      UnsafeRow agg_fastAggBuffer = null;

      if (true) {
        if (!false) {
          agg_fastAggBuffer = agg_fastHashMap.findOrInsert(
              groupKey);
        }
      }

      if (agg_fastAggBuffer == null) {
        // generate grouping key
        agg_rowWriter.write(0, groupKey);
        hashVal = 42;

        hashVal = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashInt(groupKey, hashVal);
        if (true) {
          // try to get the buffer from hash map
          agg_unsafeRowAggBuffer =
              agg_hashMap.getAggregationBufferFromUnsafeRow(agg_result1, hashVal);
        }
        if (agg_unsafeRowAggBuffer == null) {
          if (agg_sorter == null) {
            agg_sorter = agg_hashMap.destructAndCreateExternalSorter();
          } else {
            agg_sorter.merge(agg_hashMap.destructAndCreateExternalSorter());
          }

          // the hash map had be spilled, it should have enough memory now,
          // try  to allocate buffer again.
          agg_unsafeRowAggBuffer =
              agg_hashMap.getAggregationBufferFromUnsafeRow(agg_result1, hashVal);
          if (agg_unsafeRowAggBuffer == null) {
            // failed to allocate the first page
            throw new OutOfMemoryError("No enough memory for aggregation");
          }
        }
      }

      if (agg_fastAggBuffer != null) {
        // update fast row

        // common sub-expressions

        // evaluate aggregate function
        boolean agg_isNull13 = false;

        boolean agg_isNull15 = agg_fastAggBuffer.isNullAt(0);
        long agg_value16 = agg_isNull15 ? -1L : (agg_fastAggBuffer.getLong(0));
        boolean agg_isNull14 = agg_isNull15;
        long agg_value15 = agg_value16;
        if (agg_isNull14) {
          boolean agg_isNull16 = false;
          long agg_value17 = -1L;
          if (!false) {
            agg_value17 = (long) 0;
          }
          if (!agg_isNull16) {
            agg_isNull14 = false;
            agg_value15 = agg_value17;
          }
        }

        boolean agg_isNull19 = false;
        long agg_value20 = -1L;
        if (!false) {
          agg_value20 = (long) aggParam;
        }
        boolean agg_isNull18 = agg_isNull19;
        long agg_value19 = -1L;
        if (!agg_isNull19) {
          agg_value19 = agg_value20;
        }
        long agg_value14 = -1L;
        agg_value14 = agg_value15 + agg_value19;
        // update fast row
        agg_fastAggBuffer.setLong(0, agg_value14);

      } else {
        // update unsafe row

        // common sub-expressions

        // evaluate aggregate function
        boolean agg_isNull5 = false;

        boolean agg_isNull7 = agg_unsafeRowAggBuffer.isNullAt(0);
        long agg_value8 = agg_isNull7 ? -1L : (agg_unsafeRowAggBuffer.getLong(0));
        boolean agg_isNull6 = agg_isNull7;
        long agg_value7 = agg_value8;
        if (agg_isNull6) {
          boolean agg_isNull8 = false;
          long agg_value9 = -1L;
          if (!false) {
            agg_value9 = (long) 0;
          }
          if (!agg_isNull8) {
            agg_isNull6 = false;
            agg_value7 = agg_value9;
          }
        }

        boolean agg_isNull11 = false;
        long agg_value12 = -1L;
        if (!false) {
          agg_value12 = (long) aggParam;
        }
        boolean agg_isNull10 = agg_isNull11;
        long agg_value11 = -1L;
        if (!agg_isNull11) {
          agg_value11 = agg_value12;
        }
        long agg_value6 = -1L;
        agg_value6 = agg_value7 + agg_value11;
        // update unsafe row buffer
        agg_unsafeRowAggBuffer.setLong(0, agg_value6);

      }
    }

    agg_fastHashMapIter = agg_fastHashMap.rowIterator();

    agg_mapIter = agg_hashMap.iterator();
  }

  @Override
  public void init(int index, scala.collection.Iterator<InternalRow>[] iters) {

  }

  public void processNext() throws java.io.IOException {
    if (!agg_initAgg) {
      agg_initAgg = true;
      System.out.println("Starting executing...");
      Timer t = new Timer();
      agg_doAggregateWithKeys();
      t.stopAndPrint();
    }

    // output the result

    while (agg_fastHashMapIter.next()) {
      UnsafeRow key = (UnsafeRow) agg_fastHashMapIter.getKey();
      UnsafeRow value = (UnsafeRow) agg_fastHashMapIter.getValue();

      System.out.println(key.toString() + " " + value.toString());
    }
    agg_fastHashMap.close();

    while (agg_mapIter.next()) {
      UnsafeRow key = (UnsafeRow) agg_mapIter.getKey();
      UnsafeRow value = (UnsafeRow) agg_mapIter.getValue();

      System.out.println(key.toString() + " " + value.toString());
    }

    agg_mapIter.close();
    if (agg_sorter == null) {
      agg_hashMap.free();
    }
  }
}
