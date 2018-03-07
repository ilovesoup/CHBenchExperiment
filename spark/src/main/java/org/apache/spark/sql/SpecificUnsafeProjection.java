package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

public class SpecificUnsafeProjection extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {
  private UnsafeRow result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;

  public SpecificUnsafeProjection() {
    result = new UnsafeRow(1);
    this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 0);
    this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 1);

  }

  public void initialize(int partitionIndex) {

  }

  public UnsafeRow apply(InternalRow i) {
    rowWriter.zeroOutNullBytes();


    final long value = -1L;
    if (true) {
      rowWriter.setNullAt(0);
    } else {
      rowWriter.write(0, value);
    }
    return result;
  }
}
