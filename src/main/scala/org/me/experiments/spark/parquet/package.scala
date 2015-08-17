package org.me.experiments.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext(
      new SparkConf()
        .setAppName(getClass.getSimpleName)
        .setMaster("local[2]")
    )

    val profile: IntegralProfile = createIntegralProfile
    val sqlContext: SQLContext = new SQLContext(sc)

    val rdd: RDD[Row] = sc.parallelize(Seq(profile)).map(toRow)
    sqlContext.createDataFrame(rdd, createSchema)
  }

  def createSchema: StructType = {
    new StructType(null)
  }

  def createIntegralProfile: IntegralProfile = {
    val profile: IntegralProfile = new IntegralProfile()
    profile.setMetadata(createMetadata)
    profile.setIds(createIds)
    profile.setAttributes(createAttributes)
    profile.setInterests(createInterests)
    profile
  }

  def createMetadata: ProfileMetadata = {
    val metadata: ProfileMetadata = new ProfileMetadata()
    metadata.setVersion("0.1")
    metadata.setCreated(System.currentTimeMillis())
    metadata
  }

  def createIds: util.Map[String, util.Map[String, _]] = {
    val ids: util.Map[String, util.Map[String, _]] = new util.HashMap[String, util.Map[String, _]]()
    val emails: util.ArrayList[String] = new util.ArrayList[String]()
    emails.add("test@gmail.com")
    emails.add("test@hotmail.com")

    val hhIds: util.HashMap[String, Object] with Object = new util.HashMap[String, Object]()
    hhIds.put("id", "17173729")
    hhIds.put("emails", emails)
    ids.put("1", hhIds)
    ids
  }

  def createAttributes: util.Map[String, util.Map[String, Attribute]] = {
    val hhAttrs: util.Map[String, Attribute] = new util.HashMap[String, Attribute]()
    hhAttrs.put("married", new Attribute("married", true, 0.4f))
    hhAttrs.put("balance", new Attribute("balance", 1203.2d, 1.0f))
    hhAttrs.put("hasLada", new Attribute("hasLada", true, 0.5f))
    hhAttrs.put("alfabankClient", new Attribute("alfabankClient", true, 0.1f))

    val attributes: util.Map[String, util.Map[String, Attribute]] = new util.HashMap[String, util.Map[String, Attribute]]()
    attributes.put("1", hhAttrs)
    attributes
  }

  def createInterests: util.Map[String, util.Map[String, Interest]] = {
    val hhInterests: util.Map[String, Interest] = new util.HashMap[String, Interest]()
    hhInterests.put("literature", new Interest(1l, 0.3f))
    hhInterests.put("sports", new Interest(2l, 0.1f))

    val interests: util.HashMap[String, util.Map[String, Interest]] = new util.HashMap[String, util.Map[String, Interest]]()
    interests.put("1", hhInterests)
    interests
  }

  def toRow(profile: IntegralProfile): Row = {
    val row: Row = Row()
    row
  }
}

