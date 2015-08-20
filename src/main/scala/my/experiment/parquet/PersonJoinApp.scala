package my.experiment.parquet

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.me.experiments.spark.parquet.WithLocalSparkContext

case class Metadata(version: String, created: Long)

case class AttributeValue(value: String, weight: Float)

case class InterestValue(weight: Float)

case class Profile(attributes: Map[String, AttributeValue], interests: Map[String, InterestValue])

case class IntegralProfile(meta: Metadata, profiles: Map[String, Profile])

object PersonJoinApp extends WithLocalSparkContext {

  def main(args: Array[String]) {
    val sc: SparkContext = getLocalSparkContext()
    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    val hhDF: DataFrame = createHHProfiles.toDF().as("a")
//    hhDF.write.parquet("/tmp/spark-test-parquet/prq")

    val aiDF: DataFrame = createAidataProfiles.toDF().as("b")

    hhDF.join(aiDF, $"a.profiles.hh.attributes.email_prs.value" === $"b.profiles.ai.attributes.email_prs.value")
      .show()
  }

  def createHHProfiles: Seq[IntegralProfile] = {
    Seq(
      IntegralProfile(
        Metadata("1.2", System.currentTimeMillis()),
        Map("hh" -> Profile(
          Map("age" -> AttributeValue("20", 0.1f), "speciality" -> AttributeValue("programmer", 1.0f)),
          Map("bmw" -> InterestValue(1.0f), "audi" -> InterestValue(0.9f))
        ))

      ),
      IntegralProfile(
        Metadata("1.1", System.currentTimeMillis()),
        Map("hh" -> Profile(
          Map(
            "age" -> AttributeValue("11", 0.1f),
            "speciality" -> AttributeValue("programmer", 1.0f),
            "email_prs" -> AttributeValue("my@mail.com", 1.0f)
          ),
          Map("bmw" -> InterestValue(1.0f), "audi" -> InterestValue(0.9f))
        ))
      )
    )
  }

  def createAidataProfiles: Seq[IntegralProfile] = {
    Seq(
      IntegralProfile(
        Metadata("1.2", System.currentTimeMillis()),
        Map("ai" -> Profile(
          Map(
            "age" -> AttributeValue("64", 0.1f),
            "speciality" -> AttributeValue("driver", 1.0f)
          ),
          Map("bmw" -> InterestValue(1.0f), "audi" -> InterestValue(0.9f))
        ))
      ),
      IntegralProfile(
        Metadata("1.1", System.currentTimeMillis()),
        Map("ai" -> Profile(
          Map(
            "age" -> AttributeValue("52", 0.1f),
            "speciality" -> AttributeValue("programmer", 1.0f),
            "email_prs" -> AttributeValue("my@mail.com", 1.0f)
          ),
          Map("bmw" -> InterestValue(1.0f), "audi" -> InterestValue(1.0f))
        ))
      )
    )
  }
}
