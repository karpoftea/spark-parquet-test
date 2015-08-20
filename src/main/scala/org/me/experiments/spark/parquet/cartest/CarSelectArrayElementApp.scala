package org.me.experiments.spark.parquet.cartest

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.me.experiments.spark.parquet.WithLocalSparkContext

//case class Car(brend: String, model: String)
//case class Person(name: String, cars: Seq[Car])

object CarSelectArrayElementApp extends WithLocalSparkContext {

  def main(args: Array[String]) {
    val sc: SparkContext = getLocalSparkContext()
    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    val persons: Seq[Person] = createPersonSeq
    val personsDF: DataFrame = sc.parallelize(persons).toDF()
    personsDF.printSchema()
    personsDF.show()

    personsDF
      .where($"cars.brend" like "%bmw%")
      .select("name", "cars")
      .map(r => {
      println("ListOfCarsType:" + r.get(1).getClass)
      val rawCars: Seq[Any] = r.get(1).asInstanceOf[Seq[Any]]
      val bmwCars: Seq[Car] = rawCars
        .map(rawCar => rawCar.asInstanceOf[Row])
        .filter(carRow => carRow.getString(0).contains("bmw"))
        .map(carRow => Car(carRow.getString(0), carRow.getString(1)))
      Person(r.getString(0), bmwCars)
    })
      .toDF()
      .select("name", "cars.model")
      .show()
  }

  def createPersonSeq: Seq[Person] = {
    Seq(
      Person("Alice", Seq(Car("bmw", "x5"), Car("bmw", "m3"))),
      Person("Bob", Seq(Car("audi", "a6"), Car("bmw", "x6"))),
      Person("Caron", Seq(Car("audi", "a1"), Car("opel", "astra")))
    )
  }
}
