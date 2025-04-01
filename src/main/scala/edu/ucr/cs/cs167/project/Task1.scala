package edu.ucr.cs.cs167.project

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object Task1 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "count-by-county" =>
          // Sample program arguments: count-by-county Tweets_1k.tsv
          // TODO count the total number of tweets for each county and display on the screen
          val tweetsDF = sparkSession.read
            .option("header", "true")
            .option("delimiter", "\t")
            .csv(inputFile)
          //tweetsDF.show(5)
          //tweetsDF.printSchema()
          tweetsDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry")
          val tweetsRDD: SpatialRDD = tweetsDF.selectExpr("*", "ST_CreatePoint(Longitude, Latitude) AS geometry").toSpatialRDD
          val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")
          val countyTweet: RDD[(IFeature, IFeature)] = countiesRDD.spatialJoin(tweetsRDD)
          val tweetsByCounty: Map[String, Long] = countyTweet
            .map({ case (county, tweet) => (county.getAs[String]("NAME"), 1) })
            .countByKey()
          println("County\tCount")
          for ((county, count) <- tweetsByCounty)
            println(s"$county\t$count")


        case "convert" =>
          val outputFile = args(2)
          // TODO add a CountyID column to the tweets, parse the text into keywords, and write back as a Parquet file
          val tweetsRDD: SpatialRDD = sparkContext.readCSVPoint(inputFile,"Longitude","Latitude",'\t')
          val countiesDF = sparkSession.read.format("shapefile").load("tl_2018_us_county.zip")
          val countiesRDD: SpatialRDD = countiesDF.toSpatialRDD
          val tweetCountyRDD: RDD[(IFeature, IFeature)] = tweetsRDD.spatialJoin(countiesRDD)
          val tweetCounty: DataFrame = tweetCountyRDD.map({ case (tweet, county) => Feature.append(tweet, county.getAs[String]("GEOID"), "CountyID") })
            .toDataFrame(sparkSession)
          //tweetCounty.printSchema()
          val convertedDF: DataFrame = tweetCounty.selectExpr("CountyID", "split(lower(text), ',') AS keywords", "Timestamp")
          //convertedDF.printSchema()
          convertedDF.write.mode(SaveMode.Overwrite).parquet(outputFile)

        case "count-by-keyword" =>
          val keyword: String = args(2)
          // TODO count the number of occurrences of each keyword per county and display on the screen
          sparkSession.read.parquet(inputFile).createOrReplaceTempView("tweets")
          println("CountyID\tCount")
          sparkSession.sql(
            s"""
      SELECT CountyID, count(*) AS count
      FROM tweets
      WHERE array_contains(keywords, "$keyword")
      GROUP BY CountyID
    """).foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))



        case "choropleth-map" =>
          val keyword: String = args(2)
          val outputFile: String = args(3)
          // TODO write a Shapefile that contains the count of the given keyword by county
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("tweets")

          sparkSession.sql(
            s"""
      SELECT CountyID, count(*) AS count
      FROM tweets
      WHERE array_contains(keywords, "$keyword")
      GROUP BY CountyID
    """
          ).createOrReplaceTempView("keyword_counts")

          sparkSession.read.format("shapefile").load("tl_2018_us_county.zip")
            .createOrReplaceTempView("counties")

          val choroplethDF = sparkSession.sql(
            """
      SELECT CountyID, NAME, geometry, count
      FROM keyword_counts, counties
      WHERE CountyID = GEOID
    """
          )

          import edu.ucr.cs.bdlab.beast._
          val choroplethRDD: SpatialRDD = choroplethDF.toSpatialRDD

          choroplethRDD.coalesce(1).saveAsShapefile(outputFile)

        case "prepare-chicago-crimes" =>

          var df = sparkSession.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ",")
            .csv(inputFile)


          for (colName <- df.columns) {
            val newName = colName.trim.replace(" ", "")
            df = df.withColumnRenamed(colName, newName)
          }


          val dfWithGeo = df.selectExpr("*",
            "ST_CreatePoint(CAST(x AS Double), CAST(y AS Double)) AS geometry")


          val crimesRDD: SpatialRDD = dfWithGeo.toSpatialRDD.repartition(10)


          val zipRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip").repartition(10)



          val crimeZipRDD: RDD[(IFeature, IFeature)] = zipRDD.spatialJoin(crimesRDD)


          val joinedDF: DataFrame = crimeZipRDD.map { case (zip, crime) =>
            Feature.append(crime, zip.getAs[String]("ZCTA5CE10"), "ZIPCode")
          }.toDataFrame(sparkSession)


          val finalDF = joinedDF.drop("geometry")
          finalDF.printSchema()


          finalDF.repartition(10).write.mode(SaveMode.Overwrite).parquet("Chicago_Crimes_ZIP")
        //          val checkDF = sparkSession.read.parquet("Chicago_Crimes_ZIP")
        //          checkDF.show(5, truncate = false)
        //          val checkDF1 = sparkSession.read.parquet("Chicago_sample")
        //          checkDF.show(5, truncate = false)
        //          import org.apache.spark.sql.functions._
        //
        //          // A - B
        //          val diffAminusB = checkDF.except(checkDF1)
        //          // B - A
        //          val diffBminusA = checkDF1.except(checkDF)
        //
        //          if (diffAminusB.rdd.isEmpty() && diffBminusA.rdd.isEmpty()) {
        //            println("\nNo differences found. Both Parquet files have the same rows.")
        //          } else {
        //            println("\nDifferences found!")
        //
        //            println("\n--- Rows in Chicago_Crimes_ZIP but not in Chicago_sample ---")
        //            diffAminusB.show(5, truncate = false)
        //
        //            println("\n--- Rows in Chicago_sample but not in Chicago_Crimes_ZIP ---")
        //            diffBminusA.show(5, truncate = false)
        //          }


        // ---------------------------------------------------------------------------
        case _ => validOperation = false
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