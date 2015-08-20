package org.me.experiments.spark.parquet.cartest

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.me.experiments.spark.parquet.WithLocalSparkContext

case class Car(brend: String, model: String)
case class Person(name: String, cars: Seq[Car])

object CarJoinUsingArrayApp extends WithLocalSparkContext {

  def main(args: Array[String]) {
    val sc: SparkContext = getLocalSparkContext()
    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rolfClientsDF: DataFrame = sc.parallelize(createRolfPersonSeq).toDF().as("rl")
    val favoritClientsDF: DataFrame = sc.parallelize(createFavoritPersonSeq).toDF().as("fv")


    rolfClientsDF.join(favoritClientsDF, $"rl.cars.model" === $"fv.cars.model")
//      .where($"rl.cars.brend" like "")
      .show()
  }

  def createRolfPersonSeq: Seq[Person] = {
    Seq(
      Person("Alice", Seq(Car("bmw", "x5"), Car("bmw", "m3"))),
      Person("Bob", Seq(Car("audi", "a6"), Car("bmw", "x6"))),
      Person("Caron", Seq(Car("audi", "a1"), Car("opel", "astra")))
    )
  }

  def createFavoritPersonSeq: Seq[Person] = {
    Seq(
      Person("Alfa", Seq(Car("audi", "a6"))),
      Person("AliceDuplicate", Seq(Car("bmw", "x5"), Car("bmw", "m3"))),
      Person("Gamma", Seq(Car("audi", "a4"), Car("opel", "astra gtc"))),
      Person("Omega", Seq(Car("audi", "a8"), Car("bmw", "z3")))
    )
  }
}
