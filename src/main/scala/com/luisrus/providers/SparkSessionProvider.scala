package com.luisrus.providers

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

  val spark = SparkSession
    .builder()
    .appName("Test SparkSession read txt file")
    .getOrCreate()
}
