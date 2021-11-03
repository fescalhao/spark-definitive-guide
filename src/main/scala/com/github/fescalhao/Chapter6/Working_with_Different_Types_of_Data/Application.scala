package com.github.fescalhao.Chapter6.Working_with_Different_Types_of_Data

import com.github.SparkUtilsPackage.getSparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Application extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark = getSparkSession("Chapter 6 - Working with Different Types of Data")

    logger.info("Creating Data Frame")
    val df = createDataFrame(spark)

    logger.info("booleanOperations")
    booleanOperations(df)

    logger.info("numericOperations")
    numericOperations(df)

    logger.info("stringOperations")
    stringOperations(df)
  }

  def createDataFrame(spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("./datasource/2010-12-01.csv")
  }

  def booleanOperations(df: DataFrame): Unit = {
    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")
    val DOtCodeFilter = col("StockCode") === "DOT"

    // ---------------------------------------------------------------------

    df.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description")
      .show(5, truncate = false)

    df.where("InvoiceNo = 536365")
      .select("InvoiceNo", "Description")
      .show(5, truncate = false)

    // ---------------------------------------------------------------------

    df.where(col("StockCode").isin("DOT"))
      .where(priceFilter.or(descripFilter))
      .show()

    // ---------------------------------------------------------------------

    df.withColumn("isExpensive", DOtCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive")
      .show(5)

    // ---------------------------------------------------------------------

    // in case of null values
    df.where(col("Description").eqNullSafe("PACK OF 72 RETROSPOT CAKE CASES")).show()
  }

  def numericOperations(df: DataFrame): Unit = {
    val fabricatedQty = pow(col("Quantity") * col("UnitPrice"), 2) + 5

    df.select(col("CustomerId"), fabricatedQty.alias("realQuantity")).show(2)

    df.select(round(col("UnitPrice"), 1), bround(col("UnitPrice"), 1)).show(2)

    df.describe().show()

    println(df.stat.approxQuantile("UnitPrice", Array(0.5), 0.05).mkString("Array(", ", ", ")"))

    df.select(monotonically_increasing_id()).show(5)
  }

  def stringOperations(df: DataFrame): Unit = {
    df.select(
      initcap(col("Description")),
      lower(col("Description")),
      upper(col("Description"))
    ).show(2, truncate = false)

    df.select(
      ltrim(lit("   HELLO   ")).as("ltrim"),
      rtrim(lit("   HELLO   ")).as("rtrim"),
      trim(lit("   HELLO   ")).as("trim"),
      lpad(lit("HELLO"), 3, " ").as("lpad"),
      rpad(lit("HELLO"), 10, " ").as("rpad")
    ).show(1)
  }
}
