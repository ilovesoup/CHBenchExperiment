package org.apache.spark.sql.catalyst.expressions;

import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public class RowIterator implements Iterator<InternalRow> {
  public RowIterator(int length) {
    data = new int[length * 2];
    for (int i = 0; i < length; i ++) {
      data[i * 2] = i % 50 + 1;
      data[i * 2 + 1] = i % 50 + 1;
    }
    this.size = length * 2;
    this.idx = 0;
  }

  private int [] data;
  private int size;
  private int idx;

  @Override
  public boolean hasNext() {
    return idx < size;
  }

  @Override
  public InternalRow next() {
    ArrayInternalRow res = new ArrayInternalRow(data, idx);
    idx += 2;
    return res;
  }

  @Override
  public void remove() {

  }

  class ArrayInternalRow extends InternalRow {
    private final int columns[];
    private final int offset;

    ArrayInternalRow(int[] columns, int offset) {
      this.columns = columns;
      this.offset = offset;
    }

    @Override
    public int numFields() {
      return 2;
    }

    @Override
    public void setNullAt(int i) {

    }

    @Override
    public void update(int i, Object value) {

    }

    @Override
    public InternalRow copy() {
      return null;
    }

    @Override
    public boolean anyNull() {
      return false;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return false;
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return false;
    }

    @Override
    public byte getByte(int ordinal) {
      return 0;
    }

    @Override
    public short getShort(int ordinal) {
      return 0;
    }

    @Override
    public int getInt(int ordinal) {
      return columns[offset + ordinal];
    }

    @Override
    public long getLong(int ordinal) {
      return 0;
    }

    @Override
    public float getFloat(int ordinal) {
      return 0;
    }

    @Override
    public double getDouble(int ordinal) {
      return 0;
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      return null;
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      return null;
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return new byte[0];
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      return null;
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return null;
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return null;
    }

    @Override
    public MapData getMap(int ordinal) {
      return null;
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      return null;
    }
  }
}
