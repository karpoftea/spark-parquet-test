package org.me.experiments.spark.parquet

import org.apache.spark.{SparkConf, SparkContext}

trait WithLocalSparkContext {
  def getLocalSparkContext(): SparkContext = {
    new SparkContext(
      new SparkConf()
        .setAppName(getClass.getSimpleName)
        .setMaster("local[2]")
    )
  }
}
