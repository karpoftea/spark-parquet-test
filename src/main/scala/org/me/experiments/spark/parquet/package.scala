package org.me.experiments.spark

package object parquet {

  case class ProfileMetadata(version: String, created: Long, guid: String)

  case class Id(name: String, value: String)

  case class Attribute(name: String, value: String, weight: Float)

  case class Interest(taxId: Long, weight: Float)

  case class UserProfile(
//                          ids: Map[String, Id]
                          attributes: Map[String, Seq[Attribute]]
//                         , interests: Seq[Interest]
                          )

  case class IntegralProfile(metadata: ProfileMetadata, profiles: Map[String, UserProfile])
}
