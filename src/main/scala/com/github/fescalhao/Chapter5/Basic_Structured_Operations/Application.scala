package com.github.fescalhao.Chapter5.Basic_Structured_Operations

import com.github.SparkUtilsPackage.getSparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, column, desc, expr, lit}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Application extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark = getSparkSession("Chapter 5 - Basic Structured Operations")

    logger.info("createDataframe")
    val df = createDataframe(spark)

    logger.info("selectExamples")
    selectExamples(df)

    logger.info("columnBasedTransforms")
    columnBasedTransforms(df)

    logger.info("sampleTransforms")
    sampleTransforms(df)

    logger.info("unionOperation")
    unionOperation(spark, df)

    logger.info("sortingOperation")
    sortingOperation(df)
  }

  def createDataframe(spark: SparkSession, df_file: Boolean = true): DataFrame = {
    if (df_file) {
      spark
        .read
        .format("json")
        .load("./datasource/2015-summary.json")
    } else {
      val schema = StructType(List(
        StructField("some", StringType, nullable = true),
        StructField("col", StringType, nullable = true),
        StructField("names", LongType, nullable = false)
      ))

      val myRows = Seq(Row("Hello", null, 1L))

      val myRDD = spark.sparkContext.parallelize(myRows)

      spark.createDataFrame(myRDD, schema)
    }
  }

  def selectExamples(df: DataFrame): Unit = {
    df.select(
      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME").alias("DEST"),
      expr("DEST_COUNTRY_NAME as destination"))
      .show(2)

    df.selectExpr("DEST_COUNTRY_NAME as Destination", "DEST_COUNTRY_NAME").show(2)

    df.selectExpr(
      "*", // include all original columns
      "(DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME) as withinCountry"
    ).show(2)

    df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
  }

  def columnBasedTransforms(df: DataFrame): Unit = {
    // withColumn
    df.select(expr("*"), lit(1).as("One"))
      .withColumn("numberOne", lit(1))
      .withColumn("withinCountry", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME"))
      .withColumnRenamed("DEST_COUNTRY_NAME", "Destination")
      .show(2)

    // withColumn needs no escape because the first argument is just a string
    // selectExpr needs to use backticks (escape) because it's a column reference
    val newDF = df.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
      .select(col("This Long Column-Name")) // string-to-column reference
      .selectExpr("`This Long Column-Name`", "`This Long Column-Name` as `new col`")

    newDF.show(2)

    // drop
    newDF.drop(col("This Long Column-Name")).show(2)

    // cast
    df.withColumn("count2", col("count").cast("long")).show(2)

    // filter
    df.filter(col("count") < 2)
      .filter(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
      .show(2)

    // distinct takes in consideration all specified columns
    println("Distinct with DEST_COUNTRY_NAME and ORIGIN_COUNTRY_NAME: " +
      df.select(col("DEST_COUNTRY_NAME"), col("ORIGIN_COUNTRY_NAME"))
      .distinct()
      .count())

    println("Distinct with ORIGIN_COUNTRY_NAME: " +
      df.select(col("ORIGIN_COUNTRY_NAME"))
      .distinct()
      .count())
  }

  def sampleTransforms(df: DataFrame): Unit = {
    val seed = 5
    val withReplacement = false
    val fraction = 0.5
    println("Sample: " + df.sample(withReplacement, fraction, seed).count())

    val dfSplits = df.randomSplit(Array(0.25, 0.75), seed)
    println("Original df: " + df.count())
    println("dfSplits(0): " + dfSplits(0).count())
    println("dfSplits(1): " + dfSplits(1).count())
  }

  def unionOperation(spark: SparkSession, df: DataFrame): Unit = {
    val schema: StructType = df.schema
    val newRows = Seq(
      Row("New Country", "Other Country", 5L),
      Row("New Country 2", "Other Country 2", 5L),
    )

    val parallelRows = spark.sparkContext.parallelize(newRows)
    val newDF = spark.createDataFrame(parallelRows, schema)

    df.union(newDF)
      .where(col("count") === 1)
      .where(col("ORIGIN_COUNTRY_NAME") =!= "United States")
      .show()
  }

  def sortingOperation(df: DataFrame): Unit = {
    df.sort(col("count")).show(5)
    df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

    df.orderBy(desc("count")).show(2)
    df.orderBy(expr("count desc")).show(2)

    df.sortWithinPartitions(col("count")).show(2)
  }
}
