package com.luisrus.transcoding

import com.luisrus.providers.SparkSessionProvider._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Transcode {
  import spark.implicits._

  def transSap2Datalake(sapDF: DataFrame): DataFrame = {
//    val transSchema = StructType(Array(
//      StructField("sapId", IntegerType, false),
//      StructField("datalakeId", IntegerType, false),
//      StructField("driverRef", StringType, true),
//      StructField("nationality", StringType, true)
//    ))

    val transTableDF: DataFrame = spark
      .read
      .options(Map(
        "header" -> "true",
        "delimiter" -> ";"))
//      .schema(transSchema)
      .csv("C:\\Users\\Luis\\IdeaProjects\\SparkTest\\SparkTest\\src\\main\\resources\\transcoding.csv")

    val newSapDF: DataFrame = sapDF.select($"driverId", $"code", $"dob")
    newSapDF.join(transTableDF, newSapDF("driverId")===transTableDF("sapId"), "left")

  }
}
