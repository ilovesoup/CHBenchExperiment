package org.apache.spark.sql.catalyst.expressions;


import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.spark.SparkConf;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.UnifiedMemoryManager;
import org.apache.spark.sql.AggregateStandardNoNullableIterator;
import org.apache.spark.sql.SpecificUnsafeProjection;
import org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import scala.collection.immutable.Map;

public class BenchmarkTest {
  final static int bytes_in_gb = 1024 * 1024 * 1024;
  final static int num_gb = 1;
  final static int length = bytes_in_gb * num_gb / 4;
  final static int iterTimes = 8;

  public static void main(String [] args) throws Exception {
    if (Integer.parseInt(args[0]) == 0) {
      TestByJMH();
    } else {
      for (int i = 0; i < iterTimes; i++) {
        BenchmarkTest testObj = new BenchmarkTest();
        testObj.input.reset();
        testObj.iter.init(input);
        testObj.iter.process();
      }
    }
  }

  private static void TestByJMH() throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(BenchmarkTest.class.getSimpleName())
        .forks(1)
        .warmupIterations(1)
        .measurementIterations(iterTimes)
        .mode(Mode.AverageTime)
        .build();
    new Runner(opt).run();
  }

  private static RowIterator input = new RowIterator(length);

  private static AggregateStandardNoNullableIterator iter = new AggregateStandardNoNullableIterator();

  @Benchmark @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void test() throws IOException {
    input.reset();
    iter.init(input);
    iter.process();
  }

  static public UnsafeFixedWidthAggregationMap createHashMap() {
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
    SparkConf conf = new SparkConf()
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", Long.toString(bytes_in_gb * 4L));
    long maxMemory = Runtime.getRuntime().maxMemory();
    long onHeapStorageRegionSize = maxMemory / 2;
    MemoryManager memoryManager = new UnifiedMemoryManager(conf, maxMemory, onHeapStorageRegionSize, 1);
    return new TaskMemoryManager(memoryManager, 0);
  }
}
