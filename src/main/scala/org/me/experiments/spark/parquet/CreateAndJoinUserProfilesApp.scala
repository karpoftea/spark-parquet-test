package org.me.experiments.spark.parquet

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object CreateAndJoinUserProfilesApp extends WithLocalSparkContext {

  def main(args: Array[String]) {
    val sc: SparkContext = getLocalSparkContext()

    val integralProfiles: Seq[IntegralProfile] = generateIntegralProfiles()
    val ipRDD: RDD[IntegralProfile] = sc.parallelize(integralProfiles)

    val sqlContext: SQLContext = new SQLContext(sc)
    import sqlContext.implicits._

    val ipDF: DataFrame = ipRDD.toDF().as("ip")
    ipDF.printSchema()
    ipDF.show()

    val facetzProfiles: Seq[IntegralProfile] = generateFacetzProfiles()
    val facetzRDD: RDD[IntegralProfile] = sc.parallelize(facetzProfiles)
    val facetzDF: DataFrame = facetzRDD.toDF().as("fc")
    facetzDF.show()

    //join using simple field
    ipDF.join(facetzDF, ipDF("ip.metadata.guid") === facetzDF("fc.metadata.guid"))
      .map(r => {
      println(s"Input row:$r")
      val profilesRaw: collection.Map[Nothing, Nothing] = r.getMap(1) ++ r.getMap(3)
      val metadata: ProfileMetadata = toProfileMetadata(r)
      val profiles: Map[String, UserProfile] = profilesRaw.map(e => {
        val dpId: String = e._1.asInstanceOf[String]
        val userProfile: UserProfile = toUserProfile(e._2)
        (dpId, userProfile)
      }).toMap
      val profile: IntegralProfile = IntegralProfile(metadata, profiles)
      println(s"my.experiment.parquet.Profile:$profile")

      profile
    })
      .toDF()
      .write.json("/tmp/spark-parquet-test/joined-parquet")
  }

  def toProfileMetadata(r: Row): ProfileMetadata = {
    val metadataRow: Row = r.get(0).asInstanceOf[Row]
    val newMetadata: ProfileMetadata = ProfileMetadata("1.0", System.currentTimeMillis(), metadataRow.getString(2))
    newMetadata
  }

  def generateIntegralProfiles(): Seq[IntegralProfile] = {
    Seq(
      IntegralProfile(
        ProfileMetadata("1.0", System.currentTimeMillis(), "ABC")
        , Map(
          "hh" -> UserProfile(
            Map(
              "age" -> Seq(Attribute("age", "28", 1.0f)),
              "job" -> Seq(Attribute("job", "programmer", 1.0f)))
          ),
          "scorr" -> UserProfile(
            Map(
              "vkId" -> Seq(Attribute("vkId", "2637821", 1.0f), Attribute("vkId", "2324511", 0.4f)),
              "fbId" -> Seq(Attribute("fbId", "74389922232", 1.0f)),
              "hasInstagram" -> Seq(Attribute("hasInstagram", "true", 0.2f)),
              "name" -> Seq(Attribute("name", "Карпов Илья Борисович", 1.0f), Attribute("name", "Syber Clash", 0.1f))
            )
          ),
          "aidata" -> UserProfile(
            Map(
              "kamazDriver" -> Seq(Attribute("kamazDriver", "false", 1.0f)),
              "hasCar" -> Seq(Attribute("hasCar", "false", 1.0f))
            )
          )
        )
      ),
      IntegralProfile(
        ProfileMetadata("1.0", System.currentTimeMillis(), "XYZ")
        , Map(
          "hh" -> UserProfile(
            Map(
              "age" -> Seq(Attribute("age", "32", 1.0f)),
              "job" -> Seq(Attribute("job", "programmer", 1.0f)))
          ),
          "scorr" -> UserProfile(
            Map(
              "vkId" -> Seq(Attribute("vkId", "7328728", 1.0f)),
              "fbId" -> Seq(Attribute("fbId", "11122", 1.0f), Attribute("fbId", "9822672455", 1.0f)),
              "hasInstagram" -> Seq(Attribute("hasInstagram", "false", 0.2f)),
              "name" -> Seq(Attribute("name", "Токарев Саша", 1.0f), Attribute("name", "Саша", 0.1f))
            )
          ),
          "aidata" -> UserProfile(
            Map(
              "kamazDriver" -> Seq(Attribute("kamazDriver", "false", 1.0f)),
              "hasCar" -> Seq(Attribute("hasCar", "true", 1.0f))
            )
          )
        )
      )
    )
  }

  def generateFacetzProfiles(): Seq[IntegralProfile] = {
    Seq(
      IntegralProfile(
        ProfileMetadata("1.0", System.currentTimeMillis(), "ABC"),
        Map(
          "facetz" -> UserProfile(
            Map(
              "vkId" -> Seq(Attribute("vkId", "2637821", 0.9f), Attribute("vkId", "1233221", 0.1f)),
              "name" -> Seq(Attribute("vkId", "Syber Clash", 0.1f)),
              "alfabankClient" -> Seq(Attribute("alfabankClient", "true", 1.0f))
            )
          )
        )
      ),
      IntegralProfile(
        ProfileMetadata("1.0", System.currentTimeMillis(), "GVF"),
        Map(
          "facetz" -> UserProfile(
            Map(
              "vkId" -> Seq(Attribute("vkId", "16322137821", 0.9f), Attribute("vkId", "01881", 0.1f)),
              "name" -> Seq(Attribute("vkId", "Иванов Иван Иваныч", 0.1f)),
              "alfabankClient" -> Seq(Attribute("alfabankClient", "true", 0.0f))
            )
          )
        )
      )
    )
  }

  def toUserProfile(value: Any): UserProfile = {
    val row: Row = value.asInstanceOf[Row]
    println("Row:" + row)

    val attributesRaw: collection.Map[Nothing, Nothing] = row.getMap(0)
    val attributes: Map[String, Seq[Attribute]] = attributesRaw.map(a => {
      val attId: String = a._1.asInstanceOf[String]
      val attributesSeq: Seq[Any] = a._2.asInstanceOf[Seq[Any]]
      println("Seq:" + attributesSeq)

      val attributes: Seq[Attribute] = attributesSeq.map(item => {
        println("Item:" + item + " Item class:" + item.getClass)
        val attrRow: Row = item.asInstanceOf[Row]
        val attr: Attribute = Attribute(
          attrRow.getString(0),
          attrRow.getString(1),
          attrRow.getFloat(2)
        )
        attr
      })
      (attId, attributes)
    }).toMap

    UserProfile(attributes)
  }
}
