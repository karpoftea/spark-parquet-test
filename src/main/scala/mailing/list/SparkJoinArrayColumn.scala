package mailing.list

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class Attribute(name: String, value: String, weight: Float)
case class Profile(name: String, attributes: Seq[Attribute])

object SparkJoinArrayColumn {
  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName(getClass.getSimpleName))
    val sqlContext: SQLContext = new SQLContext(sc)

    import sqlContext.implicits._

    val a: DataFrame = sc.parallelize(Seq(
      Profile("Alice", Seq(Attribute("email", "alice@mail.com", 1.0f), Attribute("email", "a.jones@mail.com", 1.0f)))
    )).toDF.as("a")

    val b: DataFrame = sc.parallelize(Seq(
      Profile("Alice", Seq(Attribute("email", "alice@mail.com", 1.0f), Attribute("age", "29", 0.2f)))
    )).toDF.as("b")


    a.where($"a.attributes.name" === "email")
      .join(
        b.where($"b.attributes.name" === "email"),
        $"a.attributes.value" === $"b.attributes.value"
      )
    .show()
  }
}

/*
Hi, guys
I'm confused about joining columns in SparkSQL and need your advice.
I want to join 2 datasets of profiles. Each profile has name and array of attributes(age, gender, email etc).
There can be mutliple instances of attribute with the same name, e.g. profile has 2 emails - so 2 attributes with name = 'email' in
array. Now I want to join 2 datasets using 'email' attribute. I cant find the way to do it :(

The code is below. Now result of join is empty, while I expect to see 1 row with all Alice emails.

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class Attribute(name: String, value: String, weight: Float)
case class Profile(name: String, attributes: Seq[Attribute])

object SparkJoinArrayColumn {
  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName(getClass.getSimpleName))
    val sqlContext: SQLContext = new SQLContext(sc)

    import sqlContext.implicits._

    val a: DataFrame = sc.parallelize(Seq(
      Profile("Alice", Seq(Attribute("email", "alice@mail.com", 1.0f), Attribute("email", "a.jones@mail.com", 1.0f)))
    )).toDF.as("a")

    val b: DataFrame = sc.parallelize(Seq(
      Profile("Alice", Seq(Attribute("email", "alice@mail.com", 1.0f), Attribute("age", "29", 0.2f)))
    )).toDF.as("b")


    a.where($"a.attributes.name" === "email")
      .join(
        b.where($"b.attributes.name" === "email"),
        $"a.attributes.value" === $"b.attributes.value"
      )
    .show()
  }
}
 */
