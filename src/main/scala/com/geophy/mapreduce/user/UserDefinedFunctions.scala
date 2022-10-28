package com.geophy.mapreduce.user

import scala.util.Try

object UserDefinedFunctions {

  def mapFunction(
      key: String,  // Document name
      value: String // Document content
  ): List[(String, String)] =
    value
      .split(" ")
      .map(s => (s.toLowerCase.replaceAll("""[^\w]""", ""), "1"))
      .toList

  def reduceFunction(
      key: String,
      values: List[String]
  ): (String, String) =
    (
      key,
      values
        .foldLeft(0)((acc, next) => acc + Try(next.toInt).getOrElse(0))
        .toString
    )

  def partitionBy(key: String): String =
    key.head.toUpper.toString

}
