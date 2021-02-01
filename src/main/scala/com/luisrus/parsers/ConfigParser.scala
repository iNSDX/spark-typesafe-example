package com.luisrus.parsers

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

case class ConfigParser(configKey: String, pathToConfig: Option[String] = None){

  val conf = pathToConfig match {
    case Some(path) =>
      ConfigFactory.load(path).getConfig(configKey)
    case None =>
      ConfigFactory.load().getConfig(configKey)
  }

  val pathToSapCSV: String = {
    conf.getString("csv")
  }

  val header: String = {
    conf.getString("header")
  }

  val nullValue: String = {
    conf.getString("nullValue")
  }

  private def colTypeFromConfig(colConfigType: String): DataType = {
    colConfigType match {
      case "integer" => IntegerType
      case _ => StringType
    }
  }

  val customSchema: StructType = {
    def sortCols(firstCol: String, secondCol: String, colsConfig: Config): Boolean = {
      val firstColOrder = colsConfig.getConfig(firstCol).getInt("colOrder")
      val secondColOrder = colsConfig.getConfig(secondCol).getInt("colOrder")
      firstColOrder < secondColOrder
    }

    val colsConfig = conf.getConfig("cols")
    val cols = colsConfig.root().keySet().toArray().map(col => col.toString)
      .sortWith((firstCol, secondCol) => sortCols(firstCol, secondCol, colsConfig))
    println(cols.toList)

    StructType(
      cols.map(col => StructField(
        col,
        colTypeFromConfig(colsConfig.getConfig(col).getString("colType")))))
  }
}
