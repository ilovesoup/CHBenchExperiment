package org.apache.spark.sql;

import org.apache.log4j.Logger;

public class Timer {
  private static final Logger logger = Logger.getLogger(Timer.class);
  private long startTime;
  public Timer() {
    startTime = System.nanoTime();
  }

  public void stopAndPrint() {
    long duration = (System.nanoTime() - startTime) / 1000000;
    logger.warn("Time Elapsed " + duration + " milliseconds");
  }

  public void reset() {
    startTime = System.nanoTime();
  }
}
