package com.luisrus

import com.luisrus.providers.SparkSessionProvider._
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType}

/**
 * @author ${user.name}
 */
object ReadCSV {

  def main(args : Array[String]) {
    val customSchema = StructType(Array(
      StructField("driverId", StringType, false),
      StructField("driverRef", StringType, false),
      StructField("number", IntegerType, true),
      StructField("code", StringType, false),
      StructField("forename", StringType, false),
      StructField("surname", StringType, false),
      StructField("dob", StringType, false),
      StructField("nationality", StringType, false),
      StructField("url", StringType, false)))

    val csvFile = args(0)

    val df = spark
      .read
      .options(Map(
        "header"->"true",
        "nullValue"->"\\N"))
      .schema(customSchema) // All string if inferSchema=true by default
      .csv(csvFile)

    df.show(20, truncate = false)

  }

}
