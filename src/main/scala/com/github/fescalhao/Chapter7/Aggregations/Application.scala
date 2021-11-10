package com.github.fescalhao.Chapter7.Aggregations

import com.github.SparkUtilsPackage.getSparkSession
import com.github.fescalhao.Chapter7.Aggregations.`class`.BoolAnd
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.io.Serializable

object Application extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark = getSparkSession("Chapter 5 - Basic Structured Operations")

    logger.info("createDataframe")
    val df = createDataFrame(spark)

    logger.info("Coalesce and cache")
    df.coalesce(5).cache()

//    logger.info("aggregationOperations")
//    aggregationOperations(df)
//
//    logger.info("groupingOperations")
//    groupingOperations(df)
//
//    logger.info("windowOperations")
//    windowOperations(df)
//
//    logger.info("groupingSetsOperations")
//    groupingSetsOperations(df)

    logger.info("udafExample")
    udafExample(spark)
  }

  def createDataFrame(spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("./datasource/online-retail-dataset.csv")
  }

  def aggregationOperations(df: DataFrame): Unit = {
    df.select(
      count(col("StockCode")).alias("count"),
      countDistinct(col("StockCode")).alias("countDistinct"),
      approx_count_distinct(col("StockCode"), 0.1).alias("approxCountDistinct"),
      first(col("StockCode")).alias("first"),
      last(col("StockCode")).alias("last"),
      min(col("Quantity")).alias("min"),
      max(col("Quantity")).alias("max"),
      sum(col("Quantity")).alias("sum"),
      sumDistinct(col("Quantity")).alias("sumDistinct"),
      avg(col("Quantity")).alias("avg"),
      var_samp(col("Quantity")).alias("variance"),
      stddev_samp(col("Quantity")).alias("standardDeviation"),
      skewness(col("Quantity")).alias("skewness"),
      kurtosis(col("Quantity")).alias("kurtosis"),
      corr(col("InvoiceNo"), col("Quantity")).alias("correlation"),
      covar_samp(col("InvoiceNo"), col("Quantity")).alias("covariance"),
      collect_list(col("Country")).alias("collectList"),
      collect_set(col("Country")).alias("collectSet")
    ).show()
  }

  def groupingOperations(df: DataFrame): Unit = {
    df.groupBy(col("InvoiceNo"))
      .count()
      .show()

    df.groupBy(col("InvoiceNo"))
      .agg(
        count("Quantity").alias("count")
      ).show()

    df.groupBy(col("InvoiceNo"))
      .agg("Quantity" -> "avg", "Quantity" -> "stddev_samp")
      .show()
  }

  def windowOperations(df: DataFrame): Unit = {
    val windowSpec = Window
      .partitionBy("CustomerId", "InvoiceDateOnly")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val maxPurchaseQty = max(col("Quantity")).over(windowSpec)

    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)

    df.withColumn("InvoiceDateOnly", to_date(col("InvoiceDate"), "M/d/yyyy H:mm"))
      .where(col("CustomerId").isNotNull)
      .orderBy(col("CustomerId"))
      .select(
        col("CustomerId"),
        col("InvoiceDateOnly"),
        col("Quantity"),
        purchaseRank.alias("QuantityRank"),
        purchaseDenseRank.alias("QuantityDenseRank"),
        maxPurchaseQty.alias("MaxPurchaseQuantity")
      ).show()
  }

  def groupingSetsOperations(df: DataFrame): Unit = {
    val dfNoNull = df.drop()
      .withColumn("Date", to_date(col("InvoiceDate"), "M/d/yyyy H:mm"))

    dfNoNull.rollup(col("Date"), col("Country"))
      .agg(sum(col("Quantity")).alias("SumQuantity"))
      .select(
        col("Date"),
        col("Country"),
        col("SumQuantity")
      )
      .orderBy(col("Date"))
      .show()

    dfNoNull.cube(col("CustomerId"), col("StockCode"))
      .agg(
        grouping_id().alias("GroupingId"),
        sum(col("Quantity")).alias("SumQuantity")
      )
      .select(
        col("CustomerId"),
        col("StockCode"),
        col("GroupingId"),
        col("SumQuantity")
      )
      .orderBy(col("GroupingId").desc)
      .show()

    dfNoNull.groupBy(col("Date"))
      .pivot(col("Country"))
      .agg(sum(col("Quantity")).alias("SumQuantity"))
      .filter(col("Date") > "2011-12-05")
      .select(col("Date"), col("USA"))
      .show()
  }

  def udafExample(spark: SparkSession): Unit = {
    val boolAnd = udaf(new BoolAnd)
    spark.udf.register("booland", boolAnd)

    val myDF = spark.range(1)
      .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
      .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")

    myDF.createTempView("myDF")

    myDF.select(boolAnd(col("t")), boolAnd(col("f")))
      .show()

    spark.sql("SELECT booland(t), booland(f) FROM myDF")
      .show()
  }
}
