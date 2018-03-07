package org.apache.spark.sql;

public class Timer {
  private long startTime;
  public Timer() {
    startTime = System.nanoTime();
  }

  public void stopAndPrint() {
    long duration = (System.nanoTime() - startTime) / 1000000;
    System.out.println("Time Elapsed " + duration + " milliseconds");
  }

  public void reset() {
    startTime = System.nanoTime();
  }
}
