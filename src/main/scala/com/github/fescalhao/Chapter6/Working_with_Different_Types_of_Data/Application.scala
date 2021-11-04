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

//    logger.info("booleanOperations")
//    booleanOperations(df)
//
//    logger.info("numericOperations")
//    numericOperations(df)
//
//    logger.info("stringOperations")
//    stringOperations(df)
//
//    logger.info("dateAndTimestampOperations")
//    dateAndTimestampOperations(spark)

    logger.info("nullOperations")
    nullOperations(df)
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

    df.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description")
      .show(5, truncate = false)

    df.where("InvoiceNo = 536365")
      .select("InvoiceNo", "Description")
      .show(5, truncate = false)

    df.where(col("StockCode").isin("DOT"))
      .where(priceFilter.or(descripFilter))
      .show()

    df.withColumn("isExpensive", DOtCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive")
      .show(5)

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
    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(_.toUpperCase).mkString("|")
    val regexString2 = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("Description").contains("WHITE")
    val selectedColumns = simpleColors.map(color => {
      col("Description").contains(color.toUpperCase).alias(s"is_$color")
    }) :+ expr("*")

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

    df.select(
      regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
      col("Description")
    ).show(2)

    df.select(
      translate(col("Description"), "LEET", "1337"),
      col("Description")
    ).show(2)

    df.select(
      regexp_extract(col("Description"), regexString2, 1).alias("color_clean"),
      col("Description")
    ).show(2)

    df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .where(col("hasSimpleColor"))
      .select("Description")
      .show(3, truncate = false)

    df.select(selectedColumns: _*).where(col("is_white").or(col("is_red")))
      .select("Description")
      .show(3, truncate = false)
  }

  def dateAndTimestampOperations(spark: SparkSession): Unit = {
    val dateFormat = "yyyy-dd-MM"
    val dateDf = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    dateDf.withColumn("week_ago", date_sub(col("today"), 7))
      .withColumn("start", to_date(lit("2021-03-01")))
      .withColumn("end", to_date(lit("2021-04-22")))
      .withColumn("date1", to_date(lit("2017-12-11"), dateFormat))
      .withColumn("date2", to_date(lit("2017-20-12"), dateFormat))
      .select(
        col("today"),
        col("date1"),
        col("date2"),
        date_sub(col("today"), 5),
        date_add(col("today"), 5),
        datediff(col("today"), col("week_ago")),
        months_between(col("end"), col("start"), roundOff = true),
        col("date2") > lit("2017-12-19")
    ).show(1)
  }

  def nullOperations(df: DataFrame): Unit = {
    df.select(coalesce(col("Description"), col("CustomerId"))).show()


    df
      .na.drop() // drops any rows in which any value is null
      .na.drop("any") // drops any rows in which any value is null
      .na.drop("all") // drops rows if all values are null
      .na.drop(Seq("StockCode", "InvoiceNo")) // drops rows if specific columns are null

    df
      .na.fill("All Null values become this string for String type columns")
      .na.fill(5: Double)
      .na.fill(5, Seq("StockCode", "InvoiceNo"))
      .na.fill(Map("StockCode" -> 5, "Description" -> "No description"))
      .na.replace("Description", Map("" -> "Unknown"))
  }

  def complexTypeOperations(df: DataFrame): Unit = {

  }
}
