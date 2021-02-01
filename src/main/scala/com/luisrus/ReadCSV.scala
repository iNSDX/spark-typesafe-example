package com.luisrus

import com.luisrus.providers.SparkSessionProvider._
import com.luisrus.transcoding.Transcode._
import com.luisrus.parsers.ConfigParser

object ReadCSV {

  def main(args : Array[String]) {
    val configParser = ConfigParser(configKey = "sap")
    val csvFile = configParser.pathToSapCSV
    val customSchema = configParser.customSchema

    val sapDF = spark
      .read
      .options(Map(
        "header"->configParser.header,
        "nullValue"->configParser.nullValue))
      .schema(customSchema)
      .csv(csvFile)

    val finalDF = transSap2Datalake(sapDF, "sapId")
    finalDF.show(10, truncate = false)

  }

}
