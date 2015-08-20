package person.keyvalue.experiment

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.me.experiments.spark.parquet.WithLocalSparkContext

case class Attribute(value: String, weight: Float)
case class Person(name: String, attr: Map[String, Attribute])

//join with multiple conditions
object PersonKeyValueJoinApp extends WithLocalSparkContext {
  def main(args: Array[String]) {
    val sc: SparkContext = getLocalSparkContext()
    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    val stars: DataFrame = sc.parallelize(createSuperstars).toDF().as("ss")
    val sci: DataFrame = sc.parallelize(createPeople).toDF().as("sc")

    stars.join(
      sci, ($"ss.attr.email_prs.value" === $"sc.attr.email_prs.value")
        || ($"ss.attr.email_prs.value" === $"sc.attr.email_work.value")
        || ($"ss.attr.email_work.value" === $"sc.attr.email_work.value")
        || ($"ss.attr.email_work.value" === $"sc.attr.email_prs.value")
    )
      .show()
  }

  def createSuperstars: Seq[Person] = {
    Seq(
      Person("Arni", Map(
        "email_prs" -> Attribute("arny@gmail.com", 1.0f),
        "email_work" -> Attribute("arny@holly.com", 0.5f)
      )),
      Person("Statham", Map(
        "email_prs" -> Attribute("stat@hotmail.com", 1.0f),
        "age" -> Attribute("42", 1.0f)
      )),
      Person("Watson", Map(
        "email_work" -> Attribute("ema@london.com", 1.0f),
        "actor" -> Attribute("true", 1.0f)
      ))
    )
  }

  def createPeople: Seq[Person] = {
    Seq(
      Person("Arni-Terminator", Map(
        "muscles" -> Attribute("big", 1.0f),
        "email_prs" -> Attribute("arny@gmail.com", 0.5f)
      )),
      Person("Nash", Map(
        "muscles" -> Attribute("low", 1.0f),
        "isAustrian" -> Attribute("false", 1.0f)
      )),
      Person("Watson", Map(
        "email_prs" -> Attribute("ema@london.com", 1.0f),
        "isAustrian" -> Attribute("false", 1.0f)
      )),
      Person("James Statham", Map(
        "email_work" -> Attribute("stat@hotmail.com", 1.0f),
        "age" -> Attribute("87", 1.0f)
      ))
    )
  }
}
