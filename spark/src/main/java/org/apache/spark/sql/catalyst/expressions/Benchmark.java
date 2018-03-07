package org.apache.spark.sql.catalyst.expressions;


import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

import org.apache.spark.SparkConf;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.UnifiedMemoryManager;
import org.apache.spark.sql.SpecificUnsafeProjection;
import org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

public class Benchmark {
  final static long bytes_in_gb = 1024 * 1024 * 1024;

  public static void main(String [] args) {
    SparkConf conf = new SparkConf().set("spark.memory.offHeap.enabled", "true");
    long maxMemory = Runtime.getRuntime().maxMemory();
    long onHeapStorageRegionSize = maxMemory / 2;
    MemoryManager memoryManager = new UnifiedMemoryManager(conf, maxMemory, onHeapStorageRegionSize, 1);
    TaskMemoryManager manager = new TaskMemoryManager(memoryManager, 0);
  }

  static public UnsafeFixedWidthAggregationMap createHashMap() {
    SpecificUnsafeProjection proj = new SpecificUnsafeProjection();
    Map<String, Object> emptyMap = scala.collection.immutable.Map$.MODULE$.empty();
    Metadata emptyMeta = new Metadata(emptyMap);

    UnsafeRow initialBuffer = getEmptyAggregationBuffer();
    StructType bufferSchema = new StructType(new StructField[]{new StructField("sum", LongType, true, emptyMeta)});
    StructType groupingKeySchema = new StructType(new StructField[]{new StructField("key", IntegerType, false, emptyMeta)});

    return new UnsafeFixedWidthAggregationMap(
        initialBuffer,
        bufferSchema,
        groupingKeySchema,
        getTaskMemoryManager(),
        1024 * 16, // initial capacity
        getTaskMemoryManager().pageSizeBytes(),
        false // disable tracking of performance metrics
    );
  }

  static public UnsafeRow getEmptyAggregationBuffer() {
    SpecificUnsafeProjection proj = new SpecificUnsafeProjection();
    return proj.apply(null);
  }

  public static TaskMemoryManager getTaskMemoryManager() {
    SparkConf conf = new SparkConf().set("spark.memory.offHeap.enabled", "true");
    long maxMemory = Runtime.getRuntime().maxMemory();
    long onHeapStorageRegionSize = maxMemory / 2;
    MemoryManager memoryManager = new UnifiedMemoryManager(conf, maxMemory, onHeapStorageRegionSize, 1);
    return new TaskMemoryManager(memoryManager, 0);
  }
}
