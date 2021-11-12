package com.github.fescalhao.Chapter8.Joins

import com.github.SparkUtilsPackage
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Application extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark Session")
    val spark: SparkSession = SparkUtilsPackage.getSparkSession("Chapter 8 - Joins")
  }

}
