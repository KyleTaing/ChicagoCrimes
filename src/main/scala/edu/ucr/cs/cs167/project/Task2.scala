package edu.ucr.cs.cs167.project

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.nio.file.{Files, Paths}

object Task2 {
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: <crimeInputFile> <zipShapefilePath> <outputFile>")
      System.exit(1)
    }

    val crimeInputFile = args(0)
    val zipShapefilePath = args(1)
    val outputFile: String = args(2)


    val conf = new SparkConf().setAppName("Beast Example")
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(spark)

    try {
      val t1 = System.nanoTime()

      val crimesDF = spark.read.parquet(crimeInputFile)
      crimesDF.createOrReplaceTempView("crimes")
      val crimeAggDF = spark.sql(
        """
          |SELECT ZIPCode, count(*) AS count
          |FROM crimes
          |GROUP BY ZIPCode
          |""".stripMargin)
      crimeAggDF.createOrReplaceTempView("crime_counts")

      val zipDF = spark.read.format("shapefile").load(zipShapefilePath)
      zipDF.createOrReplaceTempView("zipcodes")

      val joinedDF = spark.sql(
        """
          |SELECT z.geometry, c.count
          |FROM zipcodes z, crime_counts c
          |WHERE z.ZCTA5CE10 = c.ZIPCode
          |""".stripMargin)

      joinedDF.coalesce(1)
        .write
        .format("shapefile")
        .save(outputFile)

      val t2 = System.nanoTime()
      println("Execution time: " + (t2 - t1) / 1e9 + " seconds")
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
