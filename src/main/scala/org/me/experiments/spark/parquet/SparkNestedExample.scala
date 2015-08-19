package org.me.experiments.spark.parquet

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class Car(brend: String, age: Int)
case class Person(name: String, cars: Map[String, Seq[Car]])

object SparkNestedExample {
  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("App"))

    val person: Person = Person(
      "Alice",
      Map("bmw" -> Seq(Car("bmw m3", 1), Car("bmw x5", 2)), "audi" -> Seq(Car("audi a6", 1)))
    )

    val rdd: RDD[Person] = sc.parallelize(Seq(person))
    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df: DataFrame = rdd.toDF()
    df.printSchema()
    df.show()

    df.selectExpr("cars.bmw.brend as brend")
      .filter($"brend" rlike  "m3" )
      .show()
  }
}
