package com.github

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source

package object SparkUtilsPackage {

  def getSparkSession(appName: String): SparkSession = {
    val sparkConf = getSparkConf(appName)

    SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
  }

  private def getSparkConf(appName: String): SparkConf = {
    val props = readSparkConfFile()
    val sparkConf = new SparkConf()
    props.forEach((k, v) => {
      sparkConf.set(k.toString, v.toString)
    })

    sparkConf.setAppName(appName)
  }

  private def readSparkConfFile(): Properties = {
    val props = new Properties()
    props.load(Source.fromFile("./spark.conf").bufferedReader())

    props
  }
}
