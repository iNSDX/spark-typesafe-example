package com.luisrus

import org.apache.spark.sql.SparkSession

import com.typesafe.config._

/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {

    val conf = ConfigFactory.load()

    val spark = SparkSession.builder().appName("Test SparkSession read txt file").getOrCreate()
    val sc = spark.sparkContext

    val data = sc.textFile("C:\\Users\\Luis\\IdeaProjects\\SparkTest\\SparkTest\\src\\main\\resources\\README.md").cache()

    val numAs = data.filter(line => line.contains("a")).count()
    val numBs = data.filter(line => line.contains("b")).count()

    println(s"Lines with a: ${numAs}, lines with b: ${numBs}")

    val confCities = conf.getConfig("cities")
    println(s"First: ${confCities.getString("andalucia")}, second: ${confCities.getString("cmadrid")}")

  }

}
