package another.experiment

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.me.experiments.spark.parquet.WithLocalSparkContext

case class Car(brend: String, model: String)
case class Person(name: String, cars: Map[String, Seq[Car]])

object PersonJoinByCarApp extends WithLocalSparkContext {
  def main(args: Array[String]) {
    val sc: SparkContext = getLocalSparkContext()
    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rolfDF: DataFrame = sc.parallelize(createRolfPersons).toDF().as("rl")
    rolfDF.printSchema()
    rolfDF.show()

    val favoritDF: DataFrame = sc.parallelize(createFavoritPersons).toDF().as("fv")
    favoritDF.printSchema()
    favoritDF.show()

    rolfDF.join(favoritDF, $"rl.cars.bmw.model" === $"fv.cars.bmw.model")
      .show()
//      .select("rl.name", "rl.cars.bmw.model", "fv.name", "fv.cars.bmw.model")
  }

  def createRolfPersons: Seq[Person] = {
    Seq(
      Person("Alice", Map("bmw" -> Seq(Car("bmw", "x5"), Car("bmw", "m3")))),
      Person("Bob", Map("audi" -> Seq(Car("audi", "a6")), "bmw" -> Seq(Car("bmw", "x6")))),
      Person("Caron", Map("audi" -> Seq(Car("audi", "a1")), "opel" -> Seq(Car("opel", "astra"))))
    )
  }

  def createFavoritPersons: Seq[Person] = {
    Seq(
      Person("Alfa", Map("bmw" -> Seq(Car("bmw", "m3")), "aston martin" -> Seq(Car("aston martin", "db3")))),
      Person("Beta", Map("audi" -> Seq(Car("audi", "a1"), Car("audi", "a8")), "bmw" -> Seq(Car("bmw", "x5")))),
      Person("Gamma", Map("ford" -> Seq(Car("ford", "focus")), "opel" -> Seq(Car("opel", "mokka")))),
      Person("AliceDuplicate", Map("bmw" -> Seq(Car("bmw", "x5"), Car("bmw", "m3"))))
    )
  }
}
