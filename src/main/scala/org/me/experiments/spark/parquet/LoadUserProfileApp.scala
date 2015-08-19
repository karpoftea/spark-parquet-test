package org.me.experiments.spark.parquet

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object LoadUserProfileApp extends WithLocalSparkContext {
  def main(args: Array[String]) {
    val sc: SparkContext = getLocalSparkContext()
    val parquet: DataFrame = new SQLContext(sc).read.parquet("/tmp/spark-parquet-test/joined")
    parquet.printSchema()
    parquet.show()

    val cache: DataFrame = parquet.cache()

    cache.select("profiles.facetz").show()
    cache.select("profiles.hh").show()
    cache.select("profiles.aidata").show()
  }
}
