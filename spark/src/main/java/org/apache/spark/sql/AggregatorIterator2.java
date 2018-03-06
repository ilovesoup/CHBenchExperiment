package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

public class AggregatorIterator2 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private boolean agg_initAgg;
  private boolean agg_bufIsNull;
  private long agg_bufValue;
  private org.apache.spark.sql.execution.aggregate.HashAggregateExec agg_plan;
  private org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap agg_hashMap;
  private org.apache.spark.sql.execution.UnsafeKVExternalSorter agg_sorter;
  private org.apache.spark.unsafe.KVIterator agg_mapIter;
  private org.apache.spark.sql.execution.metric.SQLMetric agg_peakMemory;
  private org.apache.spark.sql.execution.metric.SQLMetric agg_spillSize;
  private scala.collection.Iterator inputadapter_input;
  private UnsafeRow agg_result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder agg_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter;
  private int agg_value4;
  private UnsafeRow agg_result1;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder agg_holder1;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter1;
  private org.apache.spark.sql.execution.metric.SQLMetric wholestagecodegen_numOutputRows;
  private org.apache.spark.sql.execution.metric.SQLMetric wholestagecodegen_aggTime;

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    agg_initAgg = false;

    this.agg_plan = (org.apache.spark.sql.execution.aggregate.HashAggregateExec) references[0];

    this.agg_peakMemory = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
    this.agg_spillSize = (org.apache.spark.sql.execution.metric.SQLMetric) references[2];
    inputadapter_input = inputs[0];
    agg_result = new UnsafeRow(1);
    this.agg_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(agg_result, 0);
    this.agg_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(agg_holder, 1);

    agg_result1 = new UnsafeRow(1);
    this.agg_holder1 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(agg_result1, 0);
    this.agg_rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(agg_holder1, 1);
    this.wholestagecodegen_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[3];
    this.wholestagecodegen_aggTime = (org.apache.spark.sql.execution.metric.SQLMetric) references[4];

  }

  private void agg_doAggregateWithKeys() throws java.io.IOException {
    agg_hashMap = agg_plan.createHashMap();

    while (inputadapter_input.hasNext()) {
      InternalRow inputadapter_row = (InternalRow) inputadapter_input.next();
      boolean inputadapter_isNull = inputadapter_row.isNullAt(0);
      int inputadapter_value = inputadapter_isNull ? -1 : (inputadapter_row.getInt(0));
      boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
      long inputadapter_value1 = inputadapter_isNull1 ? -1L : (inputadapter_row.getLong(1));

      UnsafeRow agg_unsafeRowAggBuffer = null;

      UnsafeRow agg_fastAggBuffer = null;

      if (agg_fastAggBuffer == null) {
        // generate grouping key
        agg_rowWriter.zeroOutNullBytes();

        if (inputadapter_isNull) {
          agg_rowWriter.setNullAt(0);
        } else {
          agg_rowWriter.write(0, inputadapter_value);
        }
        agg_value4 = 42;

        if (!inputadapter_isNull) {
          agg_value4 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashInt(inputadapter_value, agg_value4);
        }
        if (true) {
          // try to get the buffer from hash map
          agg_unsafeRowAggBuffer =
          agg_hashMap.getAggregationBufferFromUnsafeRow(agg_result, agg_value4);
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
          agg_hashMap.getAggregationBufferFromUnsafeRow(agg_result, agg_value4);
          if (agg_unsafeRowAggBuffer == null) {
            // failed to allocate the first page
            throw new OutOfMemoryError("No enough memory for aggregation");
          }
        }
      }

      if (agg_fastAggBuffer != null) {
        // update fast row

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

        if (!inputadapter_isNull1) {
          agg_isNull6 = false; // resultCode could change nullability.
          agg_value7 = agg_value8 + inputadapter_value1;

        }
        boolean agg_isNull5 = agg_isNull6;
        long agg_value6 = agg_value7;
        if (agg_isNull5) {
          boolean agg_isNull12 = agg_unsafeRowAggBuffer.isNullAt(0);
          long agg_value13 = agg_isNull12 ? -1L : (agg_unsafeRowAggBuffer.getLong(0));
          if (!agg_isNull12) {
            agg_isNull5 = false;
            agg_value6 = agg_value13;
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

    agg_mapIter = agg_plan.finishAggregate(agg_hashMap, agg_sorter, agg_peakMemory, agg_spillSize);
  }

  protected void processNext() throws java.io.IOException {
    if (!agg_initAgg) {
      agg_initAgg = true;
      long wholestagecodegen_beforeAgg = System.nanoTime();
      agg_doAggregateWithKeys();
      wholestagecodegen_aggTime.add((System.nanoTime() - wholestagecodegen_beforeAgg) / 1000000);
    }

    // output the result

    while (agg_mapIter.next()) {
      wholestagecodegen_numOutputRows.add(1);
      UnsafeRow agg_aggKey = (UnsafeRow) agg_mapIter.getKey();
      UnsafeRow agg_aggBuffer = (UnsafeRow) agg_mapIter.getValue();

      boolean agg_isNull13 = agg_aggKey.isNullAt(0);
      int agg_value14 = agg_isNull13 ? -1 : (agg_aggKey.getInt(0));
      boolean agg_isNull14 = agg_aggBuffer.isNullAt(0);
      long agg_value15 = agg_isNull14 ? -1L : (agg_aggBuffer.getLong(0));

      agg_rowWriter1.zeroOutNullBytes();

      if (agg_isNull14) {
        agg_rowWriter1.setNullAt(0);
      } else {
        agg_rowWriter1.write(0, agg_value15);
      }
      append(agg_result1);

      if (shouldStop()) return;
    }

    agg_mapIter.close();
    if (agg_sorter == null) {
      agg_hashMap.free();
    }
  }
}
