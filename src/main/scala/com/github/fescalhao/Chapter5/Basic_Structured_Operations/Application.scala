package com.github.fescalhao.Chapter5.Basic_Structured_Operations

import com.github.SparkUtilsPackage.getSparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object Application {
  def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    logger.info("Creating spark session")
    val spark = getSparkSession("Chapter 5 - Basic Structured Operations")

    val df = createDataframe(spark)
    df.show(truncate = false)
  }

  def createDataframe(spark: SparkSession): DataFrame = {
    spark
      .read
      .format("json")
      .load("./datasource/2015-summary.json")
  }
}
