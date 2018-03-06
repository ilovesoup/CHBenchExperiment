public java.lang.Object generate(Object[] references) {
  return new SpecificUnsafeRowJoiner();
}

class SpecificUnsafeRowJoiner extends org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowJoiner {
  private byte[] buf = new byte[64];
  private UnsafeRow out = new UnsafeRow(2);

  public UnsafeRow join(UnsafeRow row1, UnsafeRow row2) {
    // row1: 1 fields, 1 words in bitset
    // row2: 1, 1 words in bitset
    // output: 2 fields, 1 words in bitset
    final int sizeInBytes = row1.getSizeInBytes() + row2.getSizeInBytes() - 8;
    if (sizeInBytes > buf.length) {
      buf = new byte[sizeInBytes];
    }

    final java.lang.Object obj1 = row1.getBaseObject();
    final long offset1 = row1.getBaseOffset();
    final java.lang.Object obj2 = row2.getBaseObject();
    final long offset2 = row2.getBaseOffset();

    Platform.putLong(buf, 16, Platform.getLong(obj1, offset1 + 0) | (Platform.getLong(obj2, offset2) << 1));

    // Copy fixed length data for row1
    Platform.copyMemory(
      obj1, offset1 + 8,
      buf, 24,
      8);


    // Copy fixed length data for row2
    Platform.copyMemory(
      obj2, offset2 + 8,
      buf, 32,
      8);


    // Copy variable length data for row1
    long numBytesVariableRow1 = row1.getSizeInBytes() - 16;
    Platform.copyMemory(
      obj1, offset1 + 16,
      buf, 40,
      numBytesVariableRow1);


    // Copy variable length data for row2
    long numBytesVariableRow2 = row2.getSizeInBytes() - 16;
    Platform.copyMemory(
      obj2, offset2 + 16,
      buf, 40 + numBytesVariableRow1,
      numBytesVariableRow2);




    out.pointTo(buf, sizeInBytes);

    return out;
  }
}
