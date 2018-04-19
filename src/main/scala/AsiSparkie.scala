import me.deviantcode.sparkie.models.Flight
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object AsiSparkie extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[4]") // only for demo and testing purposes, use spark-submit instead
    .appName("sparkie")
    .getOrCreate()

  import spark.implicits._

  try {
    val flight2007: Dataset[String] = spark.read.textFile("/tmp/flights_2007.csv.bz2")
    val header: String = flight2007.first
    val trainingData: DataFrame = flight2007
      .filter(x => x != header)
      .map(x => x.split(","))
      .filter(x => x(21) == "0")
      .filter(x => x(17) == "ORD")
      .filter(x => x(14) != "NA")
      .map(p => Flight(p(1), p(2), p(3), getMinuteOfDay(p(4)), getMinuteOfDay(p(5)), getMinuteOfDay(p(6)), getMinuteOfDay(p(7)), p(8), p(11).toInt, p(12).toInt, p(13).toInt, p(14).toDouble, p(15).toInt, p(16), p(18).toInt))
      .toDF
    trainingData.cache
    trainingData.show()

    val flight2008: Dataset[String] = spark.read.textFile("/tmp/flights_2008.csv.bz2")
    val testingData: DataFrame = flight2008
      .filter(x => x != header)
      .map(x => x.split(","))
      .filter(x => x(21) == "0")
      .filter(x => x(17) == "ORD")
      .filter(x => x(14) != "NA")
      .map(p => Flight(p(1), p(2), p(3), getMinuteOfDay(p(4)), getMinuteOfDay(p(5)), getMinuteOfDay(p(6)), getMinuteOfDay(p(7)), p(8), p(11).toInt, p(12).toInt, p(13).toInt, p(14).toDouble, p(15).toInt, p(16), p(18).toInt))
      .toDF
    testingData.cache
    testingData.show()

    //transformer to convert string to category values
    val monthIndexer: StringIndexer = new StringIndexer().setInputCol("Month").setOutputCol("MonthCat")
    val dayOfMonthIndexer: StringIndexer = new StringIndexer().setInputCol("DayofMonth").setOutputCol("DayOfMonthCat")
    val dayOfWeek: StringIndexer = new StringIndexer().setInputCol("DayOfWeek").setOutputCol("DayOfWeekCat")
    val uniqueCarrierIndexer: StringIndexer = new StringIndexer().setInputCol("UniqueCarrier").setOutputCol("UniqueCarrierCat")
    val originIndexer: StringIndexer = new StringIndexer().setInputCol("Origin").setOutputCol("OriginCat")


    // assemble raw feature
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array(
        "MonthCat", "DayOfMonthCat", "DayOfWeekCat", "UniqueCarrierCat", "OriginCat", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "DepDelay", "Distance"
      )).setOutputCol("rawFeatures")

    // VectorSlicer takes a vector as input column and create a new vector which contain only part of the attributes of the original vector.
    val vectorSlicer: VectorSlicer = new VectorSlicer()
      .setInputCol("rawFeatures")
      .setOutputCol("slicedFeatures")
      .setNames(Array("MonthCat", "DayOfMonthCat", "DayOfWeekCat", "UniqueCarrierCat", "DepTime", "ArrTime", "ActualElapsedTime", "AirTime", "DepDelay", "Distance"))

    // scale the features. StandardScaler is used to scale vector to a new vector which values are in similar scale.
    val scaler: StandardScaler = new StandardScaler()
      .setInputCol("slicedFeatures")
      .setOutputCol("features")

    // Binarizer create a binary value, 0 or 1, from input column of double value with a threshold.
    // labels for binary classifier
    val binarizerClassifier: Binarizer = new Binarizer()
      .setInputCol("ArrDelay")
      .setOutputCol("binaryLabel")
      .setThreshold(15.0)

    /*
    Use Logistic Regression Classifier
    */

    // We will initialize an estimator, the Logistic Regression Classifier, and chain them together in a machine learning pipeline
    val lr: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setLabelCol("binaryLabel")
      .setFeaturesCol("features")

    // Chain indexers and tree in a pipeline
    val lrPipeline: Pipeline = new Pipeline()
      .setStages(Array(
        monthIndexer, dayOfMonthIndexer, dayOfWeek, uniqueCarrierIndexer, originIndexer, assembler, vectorSlicer, scaler, binarizerClassifier, lr
      ))

    // train Model
    val lrModel: PipelineModel = lrPipeline.fit(trainingData)

    // Make Predictions
    val lrPredictions: DataFrame = lrModel.transform(testingData)

    // Select example rows to display.
    lrPredictions.select("prediction", "binaryLabel", "features").show(20)

    /*
    Use Decision Tree Classifier Pipeline
     */

    //index category index in raw feature
    val indexer = new VectorIndexer()
      .setInputCol("rawFeatures")
      .setOutputCol("rawFeaturesIndexed")
      .setMaxCategories(10)

    //PCA dimension reduction transformer

    val pca = new PCA()
      .setInputCol("rawFeaturesIndexed")
      .setOutputCol("features2")
      .setK(10)

    // label for multiclass classifier
    val bucketizer = new Bucketizer()
      .setInputCol("ArrDelay")
      .setOutputCol("multiClassLabel")
      .setSplits(Array(Double.NegativeInfinity, 0.0, 15.0, Double.PositiveInfinity))

    // Train a Decision Tree model
    val dt = new DecisionTreeClassifier()
      .setLabelCol("multiClassLabel")
      .setFeaturesCol("features2")

    // Chain all into a pipeline
    val dtPipeline = new Pipeline()
      .setStages(Array( monthIndexer, dayOfMonthIndexer, dayOfWeek, uniqueCarrierIndexer, originIndexer, assembler, indexer, pca, bucketizer, dt ))

    // Train model
    val dtModel = dtPipeline.fit(trainingData)

    // Make Predictions
    val dtPredictions = dtModel.transform(testingData)

    // Select example rows to display.
    dtPredictions.select("prediction", "multiClassLabel", "features").show(20)


  } catch {
    case ex: Throwable => ex.printStackTrace()

  } finally {
    spark.close()
  }

  //calculate minuted from midnight, input is military time format
  def getMinuteOfDay(depTime: String): Int = (depTime.toInt / 100).toInt * 60 + (depTime.toInt % 100)

}
