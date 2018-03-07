package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Benchmark;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;

public class AggregateStandardIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
  private scala.collection.Iterator[] inputs;
  private boolean agg_initAgg;
  private agg_FastHashMap agg_fastHashMap;
  private org.apache.spark.unsafe.KVIterator agg_fastHashMapIter;
  private org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap agg_hashMap;
  private org.apache.spark.sql.execution.UnsafeKVExternalSorter agg_sorter;
  private org.apache.spark.unsafe.KVIterator agg_mapIter;
  private scala.collection.Iterator scan_input;
  private long scan_scanTime1;
  private org.apache.spark.sql.execution.vectorized.ColumnarBatch scan_batch;
  private int scan_batchIdx;
  private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance0;
  private org.apache.spark.sql.execution.vectorized.ColumnVector scan_colInstance1;
  private UnsafeRow scan_result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder scan_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter scan_rowWriter;
  private UnsafeRow agg_result1;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder agg_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter;
  private int agg_value4;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowJoiner agg_unsafeRowJoiner;

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    wholestagecodegen_init_0();
    wholestagecodegen_init_1();

  }

  private void wholestagecodegen_init_0() {
    agg_initAgg = false;

    agg_fastHashMap = new agg_FastHashMap(Benchmark.getTaskMemoryManager(), Benchmark.getEmptyAggregationBuffer());

    scan_input = inputs[0];
    scan_scanTime1 = 0;
    scan_batch = null;
    scan_batchIdx = 0;
    scan_colInstance0 = null;
    scan_colInstance1 = null;
    scan_result = new UnsafeRow(2);
    this.scan_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(scan_result, 0);
    this.scan_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(scan_holder, 2);
    agg_result1 = new UnsafeRow(1);
    this.agg_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(agg_result1, 0);

  }

  private void scan_nextBatch() throws java.io.IOException {
    long getBatchStart = System.nanoTime();
    if (scan_input.hasNext()) {
      scan_batch = (org.apache.spark.sql.execution.vectorized.ColumnarBatch)scan_input.next();
      scan_batchIdx = 0;
      scan_colInstance0 = scan_batch.column(0);
      scan_colInstance1 = scan_batch.column(1);
    }
    scan_scanTime1 += System.nanoTime() - getBatchStart;
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
    agg_hashMap = Benchmark.createHashMap();

    if (scan_batch == null) {
      scan_nextBatch();
    }
    while (scan_batch != null) {
      int numRows = scan_batch.numRows();
      while (scan_batchIdx < numRows) {
        int scan_rowIdx = scan_batchIdx++;
        boolean scan_isNull = scan_colInstance0.isNullAt(scan_rowIdx);
        int scan_value = scan_isNull ? -1 : (scan_colInstance0.getInt(scan_rowIdx));
        boolean scan_isNull1 = scan_colInstance1.isNullAt(scan_rowIdx);
        int scan_value1 = scan_isNull1 ? -1 : (scan_colInstance1.getInt(scan_rowIdx));

        UnsafeRow agg_unsafeRowAggBuffer = null;

        UnsafeRow agg_fastAggBuffer = null;

        if (true) {
          if (!scan_isNull) {
            agg_fastAggBuffer = agg_fastHashMap.findOrInsert(
              scan_value);
          }
        }

        if (agg_fastAggBuffer == null) {
          // generate grouping key
          agg_rowWriter.zeroOutNullBytes();

          if (scan_isNull) {
            agg_rowWriter.setNullAt(0);
          } else {
            agg_rowWriter.write(0, scan_value);
          }
          agg_value4 = 42;

          if (!scan_isNull) {
            agg_value4 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashInt(scan_value, agg_value4);
          }
          if (true) {
            // try to get the buffer from hash map
            agg_unsafeRowAggBuffer =
            agg_hashMap.getAggregationBufferFromUnsafeRow(agg_result1, agg_value4);
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
            agg_hashMap.getAggregationBufferFromUnsafeRow(agg_result1, agg_value4);
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
          boolean agg_isNull16 = true;
          long agg_value17 = -1L;

          boolean agg_isNull18 = agg_fastAggBuffer.isNullAt(0);
          long agg_value19 = agg_isNull18 ? -1L : (agg_fastAggBuffer.getLong(0));
          boolean agg_isNull17 = agg_isNull18;
          long agg_value18 = agg_value19;
          if (agg_isNull17) {
            boolean agg_isNull19 = false;
            long agg_value20 = -1L;
            if (!false) {
              agg_value20 = (long) 0;
            }
            if (!agg_isNull19) {
              agg_isNull17 = false;
              agg_value18 = agg_value20;
            }
          }

          boolean agg_isNull22 = scan_isNull1;
          long agg_value23 = -1L;
          if (!scan_isNull1) {
            agg_value23 = (long) scan_value1;
          }
          boolean agg_isNull21 = agg_isNull22;
          long agg_value22 = -1L;
          if (!agg_isNull22) {
            agg_value22 = agg_value23;
          }
          if (!agg_isNull21) {
            agg_isNull16 = false; // resultCode could change nullability.
            agg_value17 = agg_value18 + agg_value22;

          }
          boolean agg_isNull15 = agg_isNull16;
          long agg_value16 = agg_value17;
          if (agg_isNull15) {
            boolean agg_isNull24 = agg_fastAggBuffer.isNullAt(0);
            long agg_value25 = agg_isNull24 ? -1L : (agg_fastAggBuffer.getLong(0));
            if (!agg_isNull24) {
              agg_isNull15 = false;
              agg_value16 = agg_value25;
            }
          }
          // update fast row
          if (!agg_isNull15) {
            agg_fastAggBuffer.setLong(0, agg_value16);
          } else {
            agg_fastAggBuffer.setNullAt(0);
          }

        } else {
          // update unsafe row

          // common sub-expressions

          // evaluate aggregate function
          boolean agg_isNull6 = true;
          long agg_value7 = -1L;

          boolean agg_isNull8 = agg_unsafeRowAggBuffer.isNullAt(0);
          long agg_value9 = agg_isNull8 ? -1L : (agg_unsafeRowAggBuffer.getLong(0));
          boolean agg_isNull7 = agg_isNull8;
          long agg_value8 = agg_value9;
          if (agg_isNull7) {
            boolean agg_isNull9 = false;
            long agg_value10 = -1L;
            if (!false) {
              agg_value10 = (long) 0;
            }
            if (!agg_isNull9) {
              agg_isNull7 = false;
              agg_value8 = agg_value10;
            }
          }

          boolean agg_isNull12 = scan_isNull1;
          long agg_value13 = -1L;
          if (!scan_isNull1) {
            agg_value13 = (long) scan_value1;
          }
          boolean agg_isNull11 = agg_isNull12;
          long agg_value12 = -1L;
          if (!agg_isNull12) {
            agg_value12 = agg_value13;
          }
          if (!agg_isNull11) {
            agg_isNull6 = false; // resultCode could change nullability.
            agg_value7 = agg_value8 + agg_value12;

          }
          boolean agg_isNull5 = agg_isNull6;
          long agg_value6 = agg_value7;
          if (agg_isNull5) {
            boolean agg_isNull14 = agg_unsafeRowAggBuffer.isNullAt(0);
            long agg_value15 = agg_isNull14 ? -1L : (agg_unsafeRowAggBuffer.getLong(0));
            if (!agg_isNull14) {
              agg_isNull5 = false;
              agg_value6 = agg_value15;
            }
          }
          // update unsafe row buffer
          if (!agg_isNull5) {
            agg_unsafeRowAggBuffer.setLong(0, agg_value6);
          } else {
            agg_unsafeRowAggBuffer.setNullAt(0);
          }

        }
        if (shouldStop()) return;
      }
      scan_batch = null;
      scan_nextBatch();
    }
    scan_scanTime1 = 0;

    agg_fastHashMapIter = agg_fastHashMap.rowIterator();

    agg_mapIter = agg_hashMap.iterator();
  }

  private void wholestagecodegen_init_1() {
    this.agg_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(agg_holder, 1);
  }

  protected void processNext() throws java.io.IOException {
    if (!agg_initAgg) {
      agg_initAgg = true;
      agg_doAggregateWithKeys();
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
