package com.github.fescalhao.Chapter8.Joins

import com.github.SparkUtilsPackage
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.Integral.Implicits.infixIntegralOps

object Application extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark Session")
    val spark: SparkSession = SparkUtilsPackage.getSparkSession("Chapter 8 - Joins")

    logger.info("Creating Data Frames")
    val dfMap = createDataFrames(spark)

    logger.info("innerJoin")
    innerJoin(dfMap)
  }

  def createDataFrames(spark: SparkSession): Map[String, DataFrame] = {
    import spark.implicits._

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)),
    )

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley")
    )

    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor")
    )

    Map(
      "personDF" -> spark.sparkContext.parallelize(person).toDF("id", "name", "graduate_program", "spark_status"),
      "graduateProgramDF" -> spark.sparkContext.parallelize(graduateProgram).toDF("id", "degree", "department", "school"),
      "sparkStatusDF" -> spark.sparkContext.parallelize(sparkStatus).toDF("id", "status")
    )
  }

  def innerJoin(dfMap: Map[String, DataFrame]): Unit = {
    val personDF: DataFrame = dfMap("personDF")
    val graduateProgramDF: DataFrame = dfMap("graduateProgramDF")

    val joinExpression = personDF.col("graduate_program") === graduateProgramDF.col("id")
    var joinType = "inner"

    personDF.join(graduateProgramDF, joinExpression, joinType).show()

    joinType = "outer"
    personDF.join(graduateProgramDF, joinExpression, joinType).show()
  }
}
