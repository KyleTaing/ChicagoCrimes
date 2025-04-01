package edu.ucr.cs.cs167.project

import org.apache.spark.SparkConf
import org.apache.spark.beast.{CRSServer, SparkSQLRegistration}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Scala examples for Beast
 */
object Task3 {
  def main(args: Array[String]): Unit = {
    // Spark session start
    val conf = new SparkConf().setAppName("Beast Example")
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)
    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext

    val operation: String = args(0)
    val inputFile: String = args(1)

    try {
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "temporal-analysis" =>
          if (args.length < 4) {
            println("Usage: temporal-analysis <inputFile> <timeUnit: year|month|day> <outputFile>")
            validOperation = false
          } else {
            val timeUnit = args(2).toLowerCase() // values: "year", "month", "day"
            val outputFile = args(3)

            // Read crime from Parquet
            val crimeDF: DataFrame = sparkSession.read.parquet(inputFile)
            crimeDF.createOrReplaceTempView("crime_data")

            // SQL query on time
            val timeColumn = timeUnit match {
              case "year"  => "YEAR(TO_TIMESTAMP(Date, 'MM/dd/yyyy hh:mm:ss a'))"
              case "month" => "CONCAT(YEAR(TO_TIMESTAMP(Date, 'MM/dd/yyyy hh:mm:ss a')), '-', MONTH(TO_TIMESTAMP(Date, 'MM/dd/yyyy hh:mm:ss a')))"
              case "day"   => "CONCAT(YEAR(TO_TIMESTAMP(Date, 'MM/dd/yyyy hh:mm:ss a')), '-', MONTH(TO_TIMESTAMP(Date, 'MM/dd/yyyy hh:mm:ss a')), '-', DAY(TO_TIMESTAMP(Date, 'MM/dd/yyyy hh:mm:ss a')))"
              case _ =>
                println("Invalid time unit. Choose from 'year', 'month', or 'day'.")
                validOperation = false
                null
            }

            if (validOperation && timeColumn != null) {
              val query =
                s"""
                   |SELECT $timeColumn AS TimePeriod, COUNT(*) AS CrimeCount
                   |FROM crime_data
                   |GROUP BY $timeColumn
                   |ORDER BY TimePeriod
                   |""".stripMargin

              val crimeTrendsDF: DataFrame = sparkSession.sql(query)

              // Saving the results in Parquet
              crimeTrendsDF.write.mode(SaveMode.Overwrite).parquet(outputFile)

              // Print
              println("TimePeriod\tCrimeCount")
              crimeTrendsDF.show(50, truncate = false)
            }
          }

        case _ =>
          println(s"Invalid operation '$operation'. Expected 'temporal-analysis'.")
          validOperation = false
      }

      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")

    } finally {
      sparkSession.stop()
    }
  }
}