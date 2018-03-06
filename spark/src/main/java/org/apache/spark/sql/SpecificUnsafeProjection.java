package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

public class SpecificUnsafeProjection extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {

  private Object[] references;
  private int value1;
  private UnsafeRow result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;

  public SpecificUnsafeProjection(Object[] references) {
    this.references = references;

    result = new UnsafeRow(1);
    this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 0);
    this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 1);

  }

  public void initialize(int partitionIndex) {

  }



  // Scala.Function1 need this
  public java.lang.Object apply(java.lang.Object row) {
    return apply((InternalRow) row);
  }

  public UnsafeRow apply(InternalRow i) {
    boolean isNull = false;

    value1 = 42;

    boolean isNull2 = i.isNullAt(0);
    int value2 = isNull2 ? -1 : (i.getInt(0));
    if (!isNull2) {
      value1 = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashInt(value2, value1);
    }

    int value = -1;

    int remainder = value1 % 200;
    if (remainder < 0) {
      value = (remainder + 200) % 200;
    } else {
      value = remainder;
    }
    rowWriter.write(0, value);
    return result;
  }
}
