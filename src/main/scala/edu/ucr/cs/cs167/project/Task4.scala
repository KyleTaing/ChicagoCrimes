package edu.ucr.cs.cs167.project

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object Task4 {
  def main(args: Array[String]): Unit = {
    if (args.length != 7) {
      println("invalid")
      System.exit(1)
    }

    // Read command-line arguments
    val parquetFile = args(0)
    val startDate = args(1)
    val endDate = args(2)
    val xMin = args(3).toDouble
    val yMin = args(4).toDouble
    val xMax = args(5).toDouble
    val yMax = args(6).toDouble

    val conf = new SparkConf().setAppName("CS167 Crime Analysis")
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    println(s"Using Spark master '${conf.get("spark.master")}'")

    // Read Parquet file
    val df = spark.read.parquet(parquetFile)

    // Convert Date column to timestamp
    val formattedDF = df
      .withColumn("timestamp", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
      .withColumn("x", col("x").cast("double"))
      .withColumn("y", col("y").cast("double"))

    /*
    formattedDF.agg(
      min("x").alias("minX"), max("x").alias("maxX"),
      min("y").alias("minY"), max("y").alias("maxY"),
      min("timestamp").alias("minDate"), max("timestamp").alias("maxDate")
    ).show()
     */

    val parsedStartDate = to_timestamp(lit(startDate), "MM/dd/yyyy")
    val parsedEndDate = to_timestamp(lit(endDate), "MM/dd/yyyy")

    /*
    formattedDF.select("Date", "timestamp").show(10, false)
    println(s"Parsed Start Date: " + parsedStartDate)
    println(s"Parsed End Date: " + parsedEndDate)
     */

    // filters
    val resultDF = formattedDF.filter(
        col("timestamp").between(parsedStartDate, parsedEndDate) &&
          col("x").between(xMin, xMax) &&
          col("y").between(yMin, yMax)
      )
      .select("x", "y", "CaseNumber", "Date")

    // Print total number of records found
    val count = resultDF.count()
    println(s"Total records matching criteria: $count")

    // Show the results (first 20)
    resultDF.show(20, false)

    // Save results
    resultDF.write
      .option("header", "true")
      .csv("RangeReportResult")

    spark.stop()
  }
}
