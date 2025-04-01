package edu.ucr.cs.cs167.project
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.{Pipeline}
import org.apache.spark.sql.{DataFrame, SparkSession}

//PART A5 - ladam020
object Task5 {
  def main(args: Array[String]): Unit = {
    // Start timer
    val t1 = System.nanoTime()

    // Spark
    val spark = SparkSession.builder
      .appName("ArrestPrediction")
      .master("local[*]")
      .getOrCreate()

    // STEP 1: Load the dataset in Parquet format
    val filePath = if (args.nonEmpty) args(0) else "Chicago_Crimes_ZIP.parquet"
    val df: DataFrame = spark.read.parquet(filePath)

    // STEP 2: Write a Spark SQL query to retrieve only those records from the dataset where the 'Arrest' column values are either 'true' nor 'false'.
    df.createOrReplaceTempView("chicago_crimes")
    val filteredDF = spark.sql("""
      SELECT *
      FROM chicago_crimes
      WHERE Arrest IN ('true','false')
    """)

    // The machine learning pipeline:
    // STEP 3: A Tokenzier that finds all the tokens (words) from the primary type and description.
    val tokenizerPrimaryType = new Tokenizer().setInputCol("PrimaryType").setOutputCol("PrimaryTypeTokens")
    val tokenizerDescription = new Tokenizer().setInputCol("Description").setOutputCol("DescriptionTokens")

    // STEP 4: A HashingTF transformer that converts the tokens into a set of numeric features.
    val hashingTFPrimaryType = new HashingTF().setInputCol("PrimaryTypeTokens").setOutputCol("PrimaryTypeFeatures").setNumFeatures(1000)
    val hashingTFDescription = new HashingTF().setInputCol("DescriptionTokens").setOutputCol("DescriptionFeatures").setNumFeatures(1000)

    // STEP 5: A StringIndexer that converts each arrest value to an index.
    val arrestIndexer = new StringIndexer().setInputCol("Arrest").setOutputCol("label")

    // Assemble only the PrimaryType and Description features into a feature vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("PrimaryTypeFeatures", "DescriptionFeatures"))
      .setOutputCol("features")
      .setHandleInvalid("skip")

    // STEP 6: A LogisticRgression or another classifier that predicts the arrest value from the set of features.
    val logRegress = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10) // Tunable
      .setRegParam(0.1) //Tunable

    // Create the pipeline
    val pipeline = new Pipeline()
      .setStages(Array(tokenizerPrimaryType, tokenizerDescription, hashingTFPrimaryType, hashingTFDescription, arrestIndexer, assembler, logRegress))

    // Hyperparameter tuning
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTFPrimaryType.numFeatures, Array(1000, 2048))
      .addGrid(hashingTFDescription.numFeatures, Array(1000, 2048))
      .addGrid(logRegress.maxIter, Array(10, 15))
      .addGrid(logRegress.regParam, Array(0.1, 0.01))
      .build()

    // Evaluator
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")

    // STEP 7: Then, You will do the regular training-test split to train on one set and test on the other.
    val tvs = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)

    // Split the data into 80% train and 20% test
    val Array(trainingData, testData) = filteredDF.randomSplit(Array(0.8, 0.2))

    // Run TrainValidationSplit
    val tvsModel: TrainValidationSplitModel = tvs.fit(trainingData)

    // Apply the best model to the test set
    val predictions: DataFrame = tvsModel.transform(testData)

    // STEP 8: Compute the total time, precision and recall of the result you found
    val evaluatorAccuracy = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluatorAccuracy.evaluate(predictions)

    val evaluatorPrecision = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("weightedPrecision")
    val precision = evaluatorPrecision.evaluate(predictions)

    val evaluatorRecall = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("weightedRecall")
    val recall = evaluatorRecall.evaluate(predictions)

    // Table creator
    val allPredictions = tvsModel.transform(filteredDF)
    allPredictions.select("PrimaryType", "Description", "Arrest", "label", "prediction").show(5) //Tunable

    // Print accuracy, precision, recall
    println(s"Accuracy on Test Set: $accuracy")
    println(s"Precision on Test Set: $precision")
    println(s"Recall on Test Set: $recall")

    // End timer
    val t2 = System.nanoTime()
    println(s"Total Execution Time: ${(t2 - t1) * 1E-9} seconds")

    spark.stop()
  }
}

